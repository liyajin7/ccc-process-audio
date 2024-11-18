#!/usr/bin/env python3

# ./0b-data-prep-auto-whisper-new-data.py -l _

import os
import gzip
import pickle

import pandas as pd

from tqdm import tqdm

import transcribe as tr


BASE_PATH = os.path.expanduser('~/github/masthesis/data/paper-round-3/')


def prep_tasks():
    TARGET = os.path.join(BASE_PATH, 'event-annotated/auto-sample-whisper-tasks-new-data.pkl')

    if os.path.exists(TARGET):
        with open(TARGET, 'rb') as f:
            tasks = pickle.load(f)
    else:
        with gzip.open(os.path.join(BASE_PATH, 'radio/new-data-processed.csv.gz'), 'rt') as f:
            audio = pd.read_csv(f)

        tasks = audio \
            [['snippet_id', 'audio_key', 'audio_file_offset', 'duration']] \
            .rename({
                'audio_key': 'key',
                'audio_file_offset': 'offset',
                'snippet_id': 'id',
            }, axis=1) \
            .sort_values('key')

        with tqdm(tasks.groupby('key'), desc='Task prep') as gby:
            tasks = {
                key: group[['id', 'offset', 'duration']]
                for key, group in gby
            }

        with open(TARGET, 'wb') as f:
            pickle.dump(tasks, f)

    return tasks


if __name__ == '__main__':
    kwargs = {
        'bucket': 'cortico-data',
        'aws_profile': 'cortico',
        'whisper_version': 'base',
        'compute_type': 'auto',
        'seed': 2969591811,
        'verbose': True,
        'outdir': os.path.join(BASE_PATH, 'event-annotated/whisper-cache/'),
    }

    tr.cli(prep_tasks=prep_tasks, **kwargs)