#!/bin/sh

if [ -n "$1" ]; then
    date_specifier="$1"
else
    date_specifier="yesterday"
fi

tweets_dir=/home/ec2-user/record/tweets/
keydir=tweets/$(date --date="$date_specifier" +"%Y/%m")
tweet_file_name=$(date --date="$date_specifier" +"%Y_%m_%d").csv
tweet_outfile_name=$(date --date="$date_specifier" +"%Y_%m_%d").csv.gz

cd $tweets_dir
gzip $tweet_file_name
export AWS_PROFILE=db_dump
aws s3 cp $tweet_file_name.gz s3://mbta-history.apptic.xyz/$keydir/$tweet_outfile_name \
    --acl public-read || exit 1
cd -
