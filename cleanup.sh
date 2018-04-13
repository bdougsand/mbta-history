#!/bin/sh

# Delete data older than 60 days.
# db_host="mbta-history.cbgw5jl0drtu.us-east-2.rds.amazonaws.com"
# psql -U bds -d $db_name -h $db_host -c "DELETE FROM vehicle_updates WHERE timestamp < (NOW() - '60' day)"

if [ -n "$BASH_SOURCE" ]; then
    default_dir="$(dirname "$BASH_SOURCE")/updates"
else
    default_dir="$HOME/mbta-history/updates"
fi

updates_dir="${MBTA_UPDATES_DIR:-$default_dir}"
when=$(date -d "10 days ago")

while true; do
    trip_start=$(date --date="$when" +"%Y-%m-%d")
    csv_filename="$updates_dir/${trip_start}.csv"
    gz_filename="$updates_dir/${trip_start}_unique.csv.gz"

    if [ ! -f "$csv_filename" ] && [ ! -f "$gz_filename" ]; then
        break
    fi

    rm "$csv_filename" || echo "'$csv_filename' not found"
    rm "$gz_filename" || echo "'$gz_filename' not found"

    when=$(date -d "$when 1 day ago")
done
