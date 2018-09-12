from datetime import datetime, time
import glob
import os
import re
import sys

import pandas as pd


def combine_summaries(file_pairs):
    return pd.concat([
        dframe.assign(trip_start=re.match(r"(\d{4}-\d{2}-\d{2})",
                                          os.path.basename(filepath)))
        for filepath, dframe in file_pairs
    ])


def summarize_route(route_df):
    return pd.Series({
        "trip_count": route_df.trip_id.nunique(),
        "count": route_df.trip_id.count(),
        "total_stops_recorded": route_df.recorded_stops.sum(),
        "median_start_delay": route_df.first_delay.median(),
        "mean_start_delay": route_df.first_delay.mean(),
        "median_end_delay": route_df.last_delay.median(),
        "mean_end_delay": route_df.last_delay.mean(),
        "mean_median_delay": route_df["delay_50"].mean()
    })


def preprocess_route():
    pass


def date_parser(dts):
    return pd.to_datetime(dts.rpartition("-")[0])


def read_summary(filepath):
    df = pd.read_csv(filepath,
                     dtype="unicode",
                     parse_dates=["scheduled_start", "scheduled_end"],
                     date_parser=date_parser)\
           .dropna(subset=["first_delay"])
    return df.assign(first_delay=pd.to_numeric(df.first_delay),
                     last_delay=pd.to_numeric(df.last_delay),
                     delay_50=pd.to_numeric(df.delay_50))


def summarize_route_files(paths, filter_fn=lambda x: x):
    combined = combine_summaries(
        (path, filter_fn(read_summary(path)))
        for path in paths
    )
    return combined.groupby("route_id").apply(summarize_route)


def rush_hour_filter(df):
    rh_morning_start_range = (time(7), time(8, 30))
    rh_morning_end_range = (time(8), time(9))

    rh_evening_start_range = (time(16), time(17, 30))
    rh_evening_end_range = (time(17), time(18, 30))

    start = df.scheduled_start.dt.time
    end = df.scheduled_end.dt.time

    return df[start.between(*rh_morning_start_range) |
              end.between(*rh_morning_start_range) |
              start.between(*rh_evening_start_range) |
              end.between(*rh_evening_end_range)]


if __name__ == "__main__":
    summarize_route_files(glob.glob("summary/*.csv.gz"),
                          rush_hour_filter)\
        .to_csv(sys.stdout)
