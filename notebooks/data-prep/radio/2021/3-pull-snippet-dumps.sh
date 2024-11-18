#!/bin/bash

set -Eeuo pipefail
set -x

cd utils

./pull_radio_reco_results.py --json --start_date 2021-03-01 --num_days 61 \
    | gzip \
    | pv \
    > ../../../../../data/raw/radio/2021.json.gz

./pull_radio_reco_results.py --json --start_date 2022-05-01 --num_days 61 \
    | gzip \
    | pv \
    > ../../../../../data/raw/radio/2022.json.gz
