#!/usr/bin/env python3

import os
import gc
import time
import gzip
import pickle
import random
import logging
import collections as cl
import multiprocessing as mp
import multiprocessing.shared_memory as shm

import numpy as np
import pandas as pd
import scipy.sparse as sp

from tqdm import tqdm


logger = logging.getLogger(__name__)


SharedMemorySpec = cl.namedtuple(
    'SharedMemorySpec',
    ['name', 'shape', 'dtype', 'nbytes']
)


def copy_to_shared_memory(arr):
    shape = arr.shape
    dtype = arr.dtype.name
    nbytes = arr.nbytes

    shr = shm.SharedMemory(create=True, size=nbytes)
    shr_arr = np.ndarray(shape, dtype=dtype, buffer=shr.buf)

    np.copyto(shr_arr, arr)

    return shr, SharedMemorySpec(shr.name, shape, dtype, nbytes)


def access_shared_memory(spec):
    shr = shm.SharedMemory(name=getattr(spec, 'name'))

    arr = np.ndarray(
        getattr(spec, 'shape'),
        dtype=getattr(spec, 'dtype'),
        buffer=shr.buf
    )

    return shr, arr


def key_to_filename(out_dir, key):
    out_path_base = [int(s) if isinstance(s, float) else s for s in key]
    out_path_base = [str(s) for s in out_path_base]
    out_path_base = '-'.join(out_path_base) + '.csv.gz'
    out_path = os.path.join(out_dir, out_path_base)

    return out_path


def sims_over_thresh(a, thresh, chunk_size=1000):
    m, n = a.shape

    i_vals, j_vals, v_vals = [], [], []

    for c in range(0, m, chunk_size):
        chunk = a[c:(c+chunk_size), :]
        sims = chunk @ a.T

        i, j = np.where(sims >= thresh)
        v = sims[i, j]
        i += c

        i_vals += [i]
        j_vals += [j]
        v_vals += [v]

    i_vals = np.concatenate(i_vals)
    j_vals = np.concatenate(j_vals)
    v_vals = np.concatenate(v_vals)

    values = sp.coo_array((v_vals, (i_vals, j_vals)), shape=(m, m), dtype=np.float64)
    values = values.asformat('csr')
    values = sp.triu(values, 1)  # matrix is symmetric + don't want self edges

    return values


def wrapper(out_dir, key, embs_shr_spec, ids_shr_spec, year_mask, mask,
            *args, **kwargs):
    year, kind, start, end = key
    out_path = key_to_filename(out_dir, key)

    embs_shr, embs_arr = access_shared_memory(embs_shr_spec)
    ids_shr, ids_arr = access_shared_memory(ids_shr_spec)
    embs_arr = embs_arr[year_mask & mask, :]

    sims = sims_over_thresh(embs_arr, thresh, *args, **kwargs)
    i, j, v = sp.find(sims)

    edges = pd.concat([
        pd.Series(ids_arr[year_mask, ...][mask][i]) \
            .rename('source').reset_index(drop=True),

        pd.Series(ids_arr[year_mask, ...][mask][j]) \
            .rename('target').reset_index(drop=True),

        pd.Series(v).rename('sim'),
    ], axis=1).assign(year=year, kind=kind)

    with gzip.open(out_path, 'wt') as f:
        edges.to_csv(f, index=False)

    del embs_arr, ids_arr
    embs_shr.close()
    ids_shr.close()

    return key


if __name__ == '__main__':
    fmt = '%(asctime)s : %(levelname)s : %(message)s'
    logging.basicConfig(
        format=fmt,

        # level=logging.INFO,
        level=logging.DEBUG,
    )

    os.chdir(os.path.expanduser('~/github/masthesis/'))

    CACHE_DIR = 'data/paper-round-3/event-annotated/auto-sample-sim-edges/'
    os.makedirs(CACHE_DIR, exist_ok=True)

    ## Params
    n_blocks = 4
    block_len = 4000  # in seconds

    # just to be safe
    seed = 2969591811
    random.seed(seed)
    np.random.seed(seed)

    ## Load data
    thresholds = pd.read_csv('data/paper-round-3/event-annotated/auto-sample-sim-thresholds.csv')

    with gzip.open('data/paper-round-3/event-annotated/auto-sample.csv.gz', 'rt') as f:
        dat = pd.read_csv(f, parse_dates=['timestamp'])
    assert dat['id'].nunique() == dat.shape[0]

    logger.debug('loaded dat')

    with open('data/paper-round-3/event-annotated/auto-sample-embeds.npy', 'rb') as f:
        embs = np.load(f)

    logger.debug('loaded embs')

    try:
        ## Set up shared memory
        ids_shr, ids_shr_spec = copy_to_shared_memory(dat['id'].to_numpy())
        embs_shr, embs_shr_spec = copy_to_shared_memory(embs)

        del embs
        gc.collect()
        logger.debug('created shared memory')

        ## Compute edges
        num_workers = 20  # mp.cpu_count() - 1
        for year in tqdm(dat['year'].unique()):
            year_mask = dat['timestamp'].dt.year == year

            year_kinds = dat.loc[year_mask, 'kind']
            year_reltimes = dat.loc[year_mask, 'reltime']
            year_reltimes -= year_reltimes.min()

            total = int(np.ceil((year_reltimes.max() - year_reltimes.min()) / block_len))
            starts = pd.Series(np.arange(total)).sample(frac=1, random_state=seed)
            ends = starts.apply(lambda p: min(p + n_blocks, total))

            for kind in tqdm(dat['kind'].unique()):
                kind_mask = (year_kinds == kind)

                thresh = thresholds.loc[(thresholds['year'] == year) & (thresholds['kind'] == kind), :]
                thresh = thresh['threshold'] - thresh['sd']  # lower threshold, more flexibility later
                thresh = thresh.item()

                with mp.Pool(processes=num_workers) as pool:
                    results = []

                    pbar = tqdm(total=len(starts))

                    for start, end in zip(starts, ends):
                        key = (year, kind, start, end)

                        time_mask = (year_reltimes >= start * block_len) & (year_reltimes <= end * block_len)
                        mask = kind_mask & time_mask
                        if mask.sum() == 0:
                            pbar.update(1)
                            continue

                        if start == starts.max() and mask.sum() < 10000:  # last batch can be small
                            pbar.update(1)
                            continue

                        if os.path.exists(key_to_filename(CACHE_DIR, key)):
                            if (
                                'SKIP_EXISTING_EDGES' in os.environ.keys() and
                                int(os.environ['SKIP_EXISTING_EDGES']) == 1
                            ):
                                pbar.update(1)
                                continue
                            else:
                                raise RuntimeError('file exists')

                        results += [pool.apply_async(
                            wrapper,
                            args=(CACHE_DIR, key, embs_shr_spec, ids_shr_spec,
                                  year_mask, mask),
                            kwds={'chunk_size': 10000},
                        )]
                    pool.close()

                    while results:
                        status = [r.ready() for r in results]
                        pbar.update(sum(status))

                        for res, stat in zip(results, status):
                            if stat:
                                res.get()  # riase if something goes wrong

                        results = [r for r in results if not r.ready()]

                        time.sleep(0.5)

                    pool.join()
    finally:
        embs_shr.close()
        ids_shr.close()

        embs_shr.unlink()
        ids_shr.unlink()
