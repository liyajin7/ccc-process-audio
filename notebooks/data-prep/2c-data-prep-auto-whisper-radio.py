#!/usr/bin/env python3

import os
import gzip
import pickle

import pandas as pd

from tqdm import tqdm

import transcribe as tr


BASE_PATH = os.path.expanduser('~/github/masthesis/data/paper-round-2/')


def prep_tasks():
    TARGET = os.path.join(BASE_PATH, 'event-annotated/auto-sample-whisper-tasks.pkl')

    if os.path.exists(TARGET):
        with open(TARGET, 'rb') as f:
            tasks = pickle.load(f)
    else:
        with gzip.open(os.path.join(BASE_PATH, 'event-annotated/auto-sample-pre-whisper.csv.gz', 'rt')) as f:
            full_sample = pd.read_csv(f)

        radio_ids = full_sample \
            .loc[full_sample['kind'] == 'radio', 'id'] \
            .apply(lambda s: int(s[1:]))

        with gzip.open(os.path.join(BASE_PATH, 'radio/paper-round-3-snippets-audio-keys.csv.gz', 'rt')) as f:
            audio = pd.read_csv(f)

        assert radio_ids.isin(audio['snippet_id']).all()
        audio = audio.loc[audio['snippet_id'].isin(radio_ids), :]

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