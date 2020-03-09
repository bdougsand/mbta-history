import io
import os
import re
import tempfile
from urllib.request import urlopen
import zipfile

import boto3

options = {}
if os.environ.get("AWS_PROFILE"):
    options = {"profile_name": os.environ["AWS_PROFILE"]}

Session = boto3.Session(**options)

from summarize import mbta_feed_url_for, process_file, CURRENT_FEED_URL


def do_summarize(bucket, key):
    S3 = Session.client("s3")

    filename = key[0:-3]

    with tempfile.NamedTemporaryFile('w', suffix=".csv.gz", delete=False) as f:
        print(f"Writing to {f.name}")
        local_file = f.name
        df = process_file(f"s3://{bucket}/{key}")
        df.to_csv(f)

    S3.upload_file(local_file, bucket, f"summary/{filename}.csv.gz")
    os.remove(local_file)


def summarize(event, context):
    record = next((rec["s3"] for rec in event["Records"]
                   if "s3" in rec), None)
    if not record:
        return

    key = record["object"]["key"]
    m = re.match(r"\d{4}/\d{2}/\d{4}-\d{2}-\d{2}\.csv\.gz$", key)
    if not m:
        return

    print(f"File {key} uploaded.")
    do_summarize(record["bucket"]["name"], key)
