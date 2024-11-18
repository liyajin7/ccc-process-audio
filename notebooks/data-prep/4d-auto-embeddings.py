#!/usr/bin/env python3

import os
import gzip
import random
import logging

import numpy as np
import pandas as pd

import sentence_transformers as st


logger = logging.getLogger(__name__)


if __name__ == '__main__':
    os.environ['TOKENIZERS_PARALLELISM'] = 'false'
    os.environ['MKL_THREADING_LAYER'] = 'GNU'

    fmt = '%(asctime)s : %(levelname)s : %(message)s'
    logging.basicConfig(format=fmt, level=logging.INFO)

    os.chdir(os.path.expanduser('~/github/masthesis/'))

    seed = 2969591811
    random.seed(seed)
    np.random.seed(seed)

    # # Load data
    with gzip.open('data/paper-round-3/event-annotated/auto-sample.csv.gz', 'rt') as f:
        dat = pd.read_csv(f, parse_dates=['timestamp'])
    assert dat['id'].nunique() == dat.shape[0]

    # ## Clean up some text formatting for a few old radio transcripts

    # radio acronyms in any non-whisper transcripts are in a format like 'n._b._a.' or 'n. b. a.' or etc and we want 'nba'
    def fix_acronym(m):
        return ' ' + m.group(0).replace('_', '').replace('.', '').replace(' ', '') + ' '
    dat.loc[(dat['kind'] == 'radio') & (dat['has_whisper'] == 0), 'content'] = dat.loc[(dat['kind'] == 'radio') & (dat['has_whisper'] == 0), 'content'].str.replace(r'(\s|\A)[a-z]\.(_[a-z]\.)*_[a-z]\.(\s|\Z)', fix_acronym, regex=True)
    dat.loc[(dat['kind'] == 'radio') & (dat['has_whisper'] == 0), 'content'] = dat.loc[(dat['kind'] == 'radio') & (dat['has_whisper'] == 0), 'content'].str.replace(r'(\s|\A)[a-z](_[a-z])*_[a-z](\s|\Z)', fix_acronym, regex=True)
    dat.loc[(dat['kind'] == 'radio') & (dat['has_whisper'] == 0), 'content'] = dat.loc[(dat['kind'] == 'radio') & (dat['has_whisper'] == 0), 'content'].str.replace(r'(\s|\A)[a-z]\.( [a-z]\.)* [a-z]\.(\s|\Z)', fix_acronym, regex=True)
    dat.loc[(dat['kind'] == 'radio') & (dat['has_whisper'] == 0), 'content'] = dat.loc[(dat['kind'] == 'radio') & (dat['has_whisper'] == 0), 'content'].str.replace(r'(\s|\A)[a-z]( [a-z])* [a-z](\s|\Z)', fix_acronym, regex=True)

    # remove underscores, undo phrase detection, needs to be after underscore stuff right above
    dat.loc[(dat['kind'] == 'radio') & (dat['has_whisper'] == 0), 'content'] = dat.loc[(dat['kind'] == 'radio') & (dat['has_whisper'] == 0), 'content'].str.replace('_', ' ')

    # we don't want to consider these at all, so just nuke 'em
    dat.loc[(dat['kind'] == 'radio') & (dat['has_whisper'] == 0), 'content'] = dat.loc[(dat['kind'] == 'radio') & (dat['has_whisper'] == 0), 'content'] \
        .str.replace('[laughter]', '', regex=False) \
        .str.replace('[noise]', '', regex=False) \
        .str.replace('[unk]', '', regex=False) \
        .str.replace('<laughter>', '', regex=False) \
        .str.replace('<noise>', '', regex=False) \
        .str.replace('<unk>', '', regex=False)

    logger.info('Finished loading data')

    # ## Embeddings
    model = st.SentenceTransformer('all-mpnet-base-v2')

    pool = model.start_multi_process_pool(target_devices=[0, 1, 2, 3])
    embs = model.encode_multi_process(dat['content'].tolist(), pool)
    model.stop_multi_process_pool(pool)

    with open('data/paper-round-3/event-annotated/auto-sample-embeds.npy', 'wb') as f:
        np.save(f, embs)
