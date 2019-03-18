#!/bin/bash

dirs="07 08 09 10 11"
for keydir in $dirs; do
    keys=$(aws s3 ls s3://mbta-history.apptic.xyz/$keydir/ | awk '{print $4}')
    for key in $keys; do
        aws s3 cp s3://mbta-history.apptic.xyz/$keydir/$key s3://mbta-history.apptic.xyz/2017/$keydir/$key
    done
done
