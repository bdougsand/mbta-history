# Requirements: pandas and pytz

# When run directly, the script expects to be in a directory containing
# descendant files of the form YYYY/mm/YYYY-mm-dd.csv.gz. It will use a
# multiprocessing pool to process each file it finds and generate outputs in
# the 'summary' subdirectory. It will also download MBTA feeds to the 'feeds'
# subdirectory as needed. To prevent file corruption, only one feed is
# downloaded at a time. This comes at the cost of some parallelism, but not
# enough to matter much.

from collections import defaultdict
import csv
from datetime import datetime, timedelta
from io import BytesIO, TextIOWrapper
import logging
from multiprocessing import Lock
import multiprocessing.pool as mp
import os
import re
import tarfile
from urllib.error import HTTPError
from urllib.request import urlopen, urlretrieve

import numpy as np
import pandas as pd
import pytz
import zipfile


CURRENT_FEED_URL = "http://www.mbta.com/uploadedfiles/MBTA_GTFS.zip"
FEED_URLS = "https://www.mbta.com/gtfs_archive/archived_feeds.txt"
FEED_DIR = os.path.join(os.getcwd(), "feeds")
TZ = pytz.timezone("US/Eastern")
_feed_urls = None

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_feed_urls():
    global _feed_urls
    if not _feed_urls:
        with urlopen(FEED_URLS) as u:
            _feed_urls = list(csv.DictReader(TextIOWrapper(BytesIO(u.read()))))
    return _feed_urls

def mbta_feed_urls():
    """Returns a generator of (feed_start_date, feed_end_date, archive_url)
    tuples from the MBTA's archived feeds site.
    """
    for l in get_feed_urls():
        yield (datetime.strptime(l["feed_start_date"], "%Y%m%d"),
               datetime.strptime(l["feed_end_date"], "%Y%m%d"),
               l["archive_url"])


def current_feed_start():
    (_start, end, _) = next(mbta_feed_urls())
    return end


def mbta_feed_urls_for(range_start=None, range_end=None):
    eastern = pytz.timezone("US/Eastern")
    range_start = range_start or datetime.now()
    range_end = range_end or range_start
    for start, end, url in mbta_feed_urls():
        if range_start.tzinfo:
            start = start.astimezone(eastern)
            end = end.astimezone(eastern)
        if start <= range_end:
            if range_start >= end:
                yield
                break
            elif end < range_start:
                continue
            yield url

def mbta_feed_url_for(when):
    """Get the URL for the MBTA's GTFS feed active at the 
    datetime `when`."""
    return next(mbta_feed_urls_for(when, when), None)


def get_zip(url=CURRENT_FEED_URL, local_basename=None):
    local_zip = os.path.join(FEED_DIR, local_basename or os.path.basename(url))
    if not os.path.exists(local_zip):
        os.makedirs(FEED_DIR, exist_ok=True)
        urlretrieve(url, local_zip)
    return zipfile.ZipFile(open(local_zip, "rb"))


def mbta_feed_for(when):
    url = mbta_feed_url_for(when)
    basename = None if url else current_feed_start().strftime("%Y%m%d.zip")
    return get_zip(url or CURRENT_FEED_URL, basename)


def get_zip_item(feed, name, dtype="unicode"):
    data = TextIOWrapper(BytesIO(feed.read(name + ".txt")), 
                         encoding="utf-8", line_buffering=True)
    return pd.read_csv(data, dtype=dtype)


DIR_PATTERN = "%Y/%m"
FILE_PATTERN = "%Y-%m-%d.csv.gz"

def date_from_filepath(filepath):
    return datetime.strptime(os.path.basename(filepath), FILE_PATTERN)

def filepath_from_date(dt):
    return dt.strftime(os.path.join(DIR_PATTERN, FILE_PATTERN))


def get_df(filepath):
    dt = date_from_filepath(filepath)
    kwargs = {}
    if dt <= datetime(2017, 8, 1):
        kwargs = {"names": ["trip_id", "trip_start", "stop_id", 
                            "stop_sequence", "vehicle_id", "status", 
                            "timestamp", "lat", "lon"]}
    df = pd.read_csv(filepath,
                     dtype="unicode",
                     **kwargs)
    df.drop_duplicates(subset=["trip_id", "timestamp"], inplace=True)
    # This turns out to be faster than using a converter
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize("UTC").dt.tz_convert(TZ)
    return df


def add_route_info(df, feed):
    trips = get_zip_item(feed, "trips")
    return pd.merge(df, trips[["trip_id", "route_id", "direction_id"]],
                    on="trip_id", how="left")


def add_schedule_times(stops, feed, outer=False):
    stop_times = get_zip_item(feed, "stop_times", dtype="unicode")
    if outer:
        stop_times = stop_times.trip_id.isin(stops.trip_id)
    return pd.merge(stops, stop_times[["trip_id", "stop_sequence", "arrival_time"]], 
                    on=["trip_id", "stop_sequence"],
                    how=("outer" if outer else "left"))


def get_date(trip_start):
    y, M, d = map(int, trip_start.split("-"))
    return TZ.localize(datetime(y, M, d).replace(hour=12)) - timedelta(hours=12)


def convert_clock_time(arrival_time, start):
    if pd.isna(arrival_time):
        return arrival_time
    h, m, s = map(int, arrival_time.split(":", 2))
    return TZ.normalize(
        start + timedelta(hours=h, minutes=m, seconds=s)
    )


def summarize_trip(trip_df, trip_start):
    name = trip_df.name
    total_stops = trip_df.arrival_time.nunique()
    seq_idx = pd.to_numeric(trip_df.stop_sequence).sort_values().index
    first_scheduled = trip_df.loc[seq_idx[0]]
    last_scheduled = trip_df.loc[seq_idx[-1]]
    scheduled_start = convert_clock_time(first_scheduled.arrival_time, trip_start)
    scheduled_end = convert_clock_time(last_scheduled.arrival_time, trip_start)

    # For each stop, select the latest timestamp recorded.
    # (Note that this will also filter out missing timestamps.)
    # Then select the remaining rows in order of stop sequence
    trip_df = trip_df.loc[trip_df.groupby("stop_sequence")["timestamp"].idxmax()]\
                     .filter(seq_idx, axis=0)
    recorded_stops = trip_df["stop_sequence"].nunique()
    data = {
        "trip_id": name,
        # Entries marked nan will be set below.
        "min_marginal_delay_stop_id": np.nan,
        "min_marginal_delay": np.nan,
        "max_marginal_delay_stop_id": np.nan,
        "max_marginal_delay": np.nan,
        "recorded_stops": recorded_stops,
        "scheduled_stops": total_stops,

        "scheduled_start": scheduled_start,
        "first_time": trip_df.iloc[0]["timestamp"],
        "first_delay": np.nan,
        "first_stop_id": trip_df.iloc[0]["stop_id"],
        "first_scheduled_stop_id": None,

        "scheduled_end": scheduled_end,
        "last_time": trip_df.iloc[-1]["timestamp"],
        "last_delay": np.nan,
        "last_stop_id": trip_df.iloc[-1]["stop_id"],
        "last_scheduled_stop_id": None,

        "delay_50": np.nan,
    }

    if total_stops and recorded_stops:
        # Exclude the case where there is recorded timestamp data for a trip
        # that is not in the static feed:
        scheduled_arrival_time = trip_df["arrival_time"].apply(convert_clock_time,
                                                               start=trip_start)
        delay = (trip_df["timestamp"] - scheduled_arrival_time).dt.total_seconds()
        marginal_delay = delay.diff()
        min_marginal = marginal_delay.idxmin()
        max_marginal = marginal_delay.idxmax()
        if not pd.isna(min_marginal):
            data.update({
                "first_delay": delay.iloc[0],
                "last_delay": delay.iloc[-1],
                "min_marginal_delay_stop_id": trip_df.loc[min_marginal]["stop_id"],
                "min_marginal_delay": marginal_delay.loc[min_marginal],
                "max_marginal_delay_stop_id": trip_df.loc[max_marginal]["stop_id"],
                "max_marginal_delay": marginal_delay.loc[max_marginal],
                "first_scheduled_stop_id": first_scheduled["stop_id"],
                "last_scheduled_stop_id": last_scheduled["stop_id"],
                "delay_50": delay.median()
            })

    return pd.Series(data)


def summarize_trips(stops, trip_start):
    return stops.groupby("trip_id").apply(summarize_trip, trip_start=trip_start)


def process_file(filepath, get_feed=mbta_feed_for):
    dt = date_from_filepath(filepath)
    trip_start = TZ.localize(datetime(dt.year, dt.month, dt.day).replace(hour=12)) - timedelta(hours=12)

    feed = get_feed(dt)
    return add_route_info(
        summarize_trips(
            add_schedule_times(get_df(filepath), feed),
            trip_start),
        feed)


def getpaths(indir, outdir):
    for d, subdirs, files in os.walk(indir, topdown=True):
        if os.path.samefile(d, indir):
            for i in range(len(subdirs)-1, -1, -1):
                if not re.match(r"\d{4}", subdirs[i]):
                    del subdirs[i]
            continue
        for filename in files:
            m = re.match(r"(\d{4}-\d{2}-\d{2})\.csv\.gz$", filename)
            if m:
                yield (os.path.join(d, filename),
                       os.path.join(outdir, "{ymd}.csv".format(ymd=m.group(1))))


def init(dl_lock):
    global lock
    lock = dl_lock


def get_feed_locking(when):
    lock.acquire()
    try:
        return mbta_feed_for(when)
    finally:
        lock.release()


def do_process(args):
    (filepath, outpath) = args
    print(f"Processing {filepath}")
    try:
        process_file(filepath, get_feed_locking).to_csv(outpath)
        print(f"Wrote to {outpath}")
    except Exception as exc:
        logger.exception("Processing failed for %s", filepath)


def process_all(indir=".", outdir="./summary"):
    dl_lock = Lock()
    with mp.Pool(initializer=init, initargs=(dl_lock,)) as pool:
        pool.map(do_process, getpaths(indir, outdir))


if __name__ == "__main__":
    process_all()
