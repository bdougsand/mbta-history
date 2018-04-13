#!/bin/sh

if [ -n "$1" ]; then
    date_specifier="$1"
else
    date_specifier="yesterday"
fi

keydir=$(date --date="$date_specifier" +"%Y/%m")
trip_start=$(date --date="$date_specifier" +"%Y-%m-%d")

if [ -n "$BASH_SOURCE" ]; then
    default_dir="$(dirname "$BASH_SOURCE")/updates"
else
    default_dir="$HOME/mbta-history/updates"
fi

updates_dir="${MBTA_UPDATES_DIR:-$default_dir}"

cd "$updates_dir"
cat $trip_start.csv | uniq > ${trip_start}_unique.csv
gzip ${trip_start}_unique.csv
export AWS_PROFILE=db_dump
aws s3 cp ${trip_start}_unique.csv.gz \
    s3://mbta-history.apptic.xyz/$keydir/$trip_start.csv.gz \
    --acl public-read || exit 1
rm $trip_start.csv
cd -
