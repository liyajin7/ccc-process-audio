#!/bin/bash

set -Eeuo pipefail
set -x

cat tmp/targets.txt \
    | ./utils/glacier.py -b cortico-data -a cortico -v -s 2969591811 \
    > tmp/glacier-restore.log
