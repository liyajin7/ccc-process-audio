#!/bin/bash

set -xe

AWS_PROFILE="cortico"
BUCKET="cortico-data"
URL_PREFIX="s3://$BUCKET/speechbox/stream_out"

mkdir -p tmp
rm -f tmp/targets.txt

while IFS= read -r line; do
    aws s3 ls --recursive --profile="$AWS_PROFILE" "$URL_PREFIX/$line/" | awk '{print $4}' >> tmp/targets.txt
done < target-dates.txt
