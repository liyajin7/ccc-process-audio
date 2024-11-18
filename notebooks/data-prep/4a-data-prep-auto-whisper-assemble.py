#!/usr/bin/env python3

import os
import gzip
import json
import logging

import pandas as pd

from tqdm import tqdm


logger = logging.getLogger(__name__)


if __name__ == '__main__':
    fmt = '%(asctime)s : %(levelname)s : %(message)s'
    logging.basicConfig(format=fmt, level=logging.INFO)

    BASE_PATH = os.path.expanduser('~/github/masthesis/data/paper-round-3')
    CACHE_DIR = os.path.join(BASE_PATH, 'event-annotated/whisper-cache/')
    TARGET = os.path.join(BASE_PATH, 'event-annotated/auto-sample-whisper-transcripts.csv.gz')

    transcribed = []
    with tqdm() as pbar:
        for root, dirs, files in os.walk(CACHE_DIR):
            for file in files:
                try:
                    with open(os.path.join(root, file), 'rt') as f:
                        content = json.load(f)

                    transcribed += [{
                        'snippet_id': content['id'],
                        'content': ' '.join([s['text'] for s in content['segments']]).strip(),
                    }]
                except Exception as exc:
                    logger.exception(f'Failed to read file {file}')
                finally:
                    pbar.update(1)

    with gzip.open(TARGET, 'wt') as f:
        pd.DataFrame(transcribed).to_csv(f, index=False)
