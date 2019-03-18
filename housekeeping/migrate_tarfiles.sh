#!/bin/bash

set -e

export AWS_PROFILE=db_dump
uploaded=$(aws s3api list-objects \
               --bucket "mbta-history.apptic.xyz" \
               --prefix "2017/" |
               jq -r '.Contents[].Key | select(endswith("tgz"))')

mkdir -p /tmp/s3cleanup
cd /tmp/s3cleanup
for item_key in $uploaded; do
    wget http://mbta-history.apptic.xyz/$item_key || exit 1
    filename=$(basename $item_key)
    csv_filename=$(echo $filename | cut -f 1-2 -d .)
    tar xf $filename
    gzip $csv_filename
    new_key=$(echo $item_key | cut -f 1-2 -d .).gz
    aws s3 cp $csv_filename.gz s3://mbta-history.apptic.xyz/$new_key --acl public-read || exit 1
    aws s3 rm s3://mbta-history.apptic.xyz/$item_key
    rm $filename
    rm $csv_filename.gz
done
