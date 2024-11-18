#!/usr/bin/env python3

import os
import gzip
import random
import pickle
import logging
import hashlib
import argparse

import torch
import numpy as np
import pandas as pd

from tqdm import tqdm

import transcribe as tr


BASE_PATH = os.path.expanduser('~/github/masthesis/data/paper-round-2/')
CACHE_DIR = os.path.join(BASE_PATH, 'event-annotated/whisper-cache/')


def prep_data(target='event-annotated/auto-sample-whisper-tasks.pkl'):
    target = os.path.join(BASE_PATH, target)

    if os.path.exists(target):
        with open(target, 'rb') as f:
            return pickle.load(f)

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

    return tasks


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--cuda', action='store_true',
                        help='auto-select and use GPUs (default CPU)')
    parser.add_argument('-b', '--bucket', default='cortico-data')
    parser.add_argument('-a', '--aws-profile', default='cortico')
    parser.add_argument('-t', '--compute-type', default='auto')
    parser.add_argument('-w', '--whisper-version', default='base')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-s', '--seed', default=2969591811, type=int)

    parser.add_argument('-n', '--n-splits', default=1, type=int)
    parser.add_argument('-l', '--split', default=0, type=int)

    args = parser.parse_args()

    ## Set seeds
    random.seed(args.seed)
    np.random.seed(args.seed)
    torch.manual_seed(args.seed)

    ## Logging
    logging.basicConfig(
        format='%(asctime)s : %(levelname)s : %(message)s',
        level=logging.INFO if args.verbose else logging.WARNING,
    )

    logger = logging.getLogger(__name__)
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)

    ## What class / params to use?
    kls = tr.CudaTranscriber if args.cuda else tr.MultiTranscriber
    params = {
        'cache_only': True,
        'progress': True,

        'bucket': args.bucket,
        'aws_profile': args.aws_profile,

        'whisper_version': args.whisper_version,
        'compute_type': args.compute_type,

        'cache_dir': CACHE_DIR,
    }

    if args.cuda:
        params['cuda_devices'] = 'auto'
    else:
        params['n_procs'] = 20
        params['cuda_devices'] = []
        params['gpu_share'] = 0

    ## Set up data
    tasks = prep_data()

    keys = sorted(list(tasks.keys()))
    splits = [
        int(hashlib.sha256(k.encode('utf-8')).hexdigest(), 16) % args.n_splits
        for k in keys
    ]
    selected_keys = [k for k, s in zip(keys, splits) if s == args.split]

    params['tasks'] = {k: tasks[k] for k in selected_keys}

    ## Run the job!
    kls(**params).run()
