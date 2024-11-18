#!/usr/bin/env python

import os
import gzip
import logging

import pandas as pd

import glacier as gl


logger = logging.getLogger(__name__)


def prep_keys():
    BASE_PATH = os.path.expanduser('~/github/masthesis/data/paper-round-3/')

    full_sample = pd.read_csv(os.path.join(BASE_PATH, 'event-annotated/auto-sample-pre-whisper.csv.gz')) \
        .loc[full_sample['kind'] == 'radio', 'id'] \
        .apply(lambda s: int(s[1:]))

    audio = pd.read_csv(os.path.join(BASE_PATH, 'radio/paper-round-3-snippets-audio-keys.csv.gz'))

    assert radio_ids.isin(audio['snippet_id']).all()
    audio = audio.loc[audio['snippet_id'].isin(radio_ids), :]

    return audio['audio_key'].tolist()


if __name__ == '__main__':
    kwargs = {
        'bucket': 'cortico-data',
        'aws_profile': 'cortico',
        'concurrency': 2 * os.cpu_count(),
        'verbose': False,
        'seed': 2969591811,
    }

    gl.cli(keys=prep_keys(), **kwargs)
