import csv
from datetime import datetime, timedelta
import json
import os
import pytz
import time

from TwitterAPI import TwitterAPI, TwitterRestPager

from config import TwitterCredentials as config


api = TwitterAPI(config["consumer_key"],
                 config["consumer_secret"],
                 config["access_token"],
                 config["access_token_secret"])


def parse_date(datestr):
    return datetime.strptime(datestr, "%a %b %d %H:%M:%S %z %Y")


def hashtags(status):
    return ",".join([ht["text"] for ht in status["entities"]["hashtags"]])


def get_recent_tweets(q, since_dt=None, since_id=None):
    if not (since_dt or since_id):
        since_dt = pytz.utc.localize(datetime.utcnow()-timedelta(hours=1))
    params = {"q": q, "count": 100}
    if since_id:
        params["since_id"] = since_id

    r = TwitterRestPager(api, "search/tweets", params)
    for item in r.get_iterator():
        when = parse_date(item["created_at"])
        if since_dt and when < since_dt:
            return
        # Ignore retweets:
        if "retweeted_status" in item:
            continue
        yield {"id": item["id"],
               "when": when.isoformat(),
               "hashtags": hashtags(item),
               "text": item["text"],
               "geo": item["geo"],
               "retweet_count": item["retweet_count"],
               "favorite_count": item["favorite_count"],
               "source": item["source"],
               "user": item["user"]["id"]}


def hour_start(dt):
    return dt.replace(minute=0, second=0, microsecond=0)


def fetch_tweets():
    last_id = None
    timezone = pytz.timezone("US/Eastern")
    base_dir = os.path.dirname(__file__)

    while True:
        now = datetime.utcnow()
        local_now = pytz.utc.localize(now).astimezone(timezone)
        file_name = local_now.strftime("%Y_%m_%d.csv")
        file_path = os.path.join(base_dir, "tweets", file_name)
        exists = os.path.exists(file_path)
        with open(file_path, "a") as out:
            writer = csv.DictWriter(out, ["id", "when", "hashtags", "text",
                                          "geo", "retweet_count",
                                          "favorite_count", "source", "user"])
            if not exists:
                writer.writeheader()
            for tweet in get_recent_tweets("@MBTA"):
                writer.writerow(tweet)
                last_id = tweet["id"]
        next_run = hour_start(now) + timedelta(hours=1)
        time.sleep((next_run - datetime.utcnow()).total_seconds())


if __name__ == "__main__":
    fetch_tweets()
