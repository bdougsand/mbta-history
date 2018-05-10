#!/bin/bash

if [ -n "$1" ]; then cd "$1"; fi

file_pattern='([0-9]{4})-([0-9]{2})-[0-9][0-9]\.csv'

for csv_file in *.csv; do
    if [[ $csv_file =~ $file_pattern ]]; then
        echo "Processing $csv_file"
        year="${BASH_REMATCH[1]}"
        month="${BASH_REMATCH[2]}"
        uniq < $csv_file | gzip > $csv_file.gz
        aws s3 cp $csv_file.gz \
            s3://mbta-history.apptic.xyz/$year/$month/$csv_file.gz \
            --acl public-read || exit 1
        rm $csv_file.gz
        rm $csv_file
    fi
done
