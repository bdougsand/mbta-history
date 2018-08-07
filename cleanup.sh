#!/bin/sh

if [ -n "$BASH_SOURCE" ]; then
    default_dir="$(dirname "$BASH_SOURCE")/updates"
else
    default_dir="$HOME/mbta-history/updates"
fi

if [ -n "$1" ]; then
    when=$(date -d "$1") || exit 1
else
    when=$(date -d "10 days ago")
fi

updates_dir="${MBTA_UPDATES_DIR:-$default_dir}"

echo Cleaning up files from $(date --date="$when" +"%B %d, %Y") and before
cleanup_count=0

while true; do
    trip_start=$(date --date="$when" +"%Y-%m-%d")
    csv_filename="$updates_dir/${trip_start}.csv"
    gz_filename="$updates_dir/${trip_start}_unique.csv.gz"

    if [ ! -f "$csv_filename" ] && [ ! -f "$gz_filename" ]; then
        break
    fi

    rm "$csv_filename" && echo "Deleted '$csv_filename'" && cleanup_count=$((cleanup_count+1))
    rm "$gz_filename" && echo "Deleted '$gz_filename'" && cleanup_count=$((cleanup_count+1))

    when=$(date -d "$when 1 day ago")
done

echo "Cleaned up $cleanup_count files"
