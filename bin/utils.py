import os
import sys
import csv
import json
import gzip
import time
import ctypes
import string
import shutil
import base64
import random
import pickle
import logging
import hashlib
import tempfile
import contextlib
import pathlib as pl
import datetime as dt
import itertools as it
import collections as cl
import multiprocessing as mp
from abc import ABC, abstractmethod

import psycopg2
import psycopg2.extras
import nltk.corpus as cp

from psycopg2 import sql
from tqdm import tqdm
from tqdm.notebook import tqdm as tqdm_notebook

import numpy as np
import pandas as pd
import scipy.sparse as ss
import sklearn as sk

from sklearn.decomposition import TruncatedSVD
from urllib.parse import urlencode, quote_plus

fmt = '%(asctime)s : %(module)s : %(levelname)s : %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO)
logger = logging.getLogger(__name__)

##
## Links to the transcript-explore tool
##

# Wrapper for serving a recognition result file
TRANSCRIPT_URL = "https://radiosearch-backend.cortico.ai/audio_key_reco_result/?%s"

# Wrapper for playing an audio file in cortico-data s3
PLAY_URL = "https://radiosearch-backend.cortico.ai/play_audio_key/?%s"

# Transcript explore base URL
TRANSCRIPT_EXPLORE_URL = "https://prototypes.cortico.ai/transcript-explore/?%s"

def transcript_explore_url(audio_key, t=0):
    tsv_url = TRANSCRIPT_URL % (urlencode({"audio_key": audio_key},
                                quote_via=quote_plus))

    play_url = PLAY_URL % (urlencode({"audio_key": audio_key}))
    params = {"audioUrl": play_url, "transcriptUrl": tsv_url, "t": t}

    return TRANSCRIPT_EXPLORE_URL % (urlencode(params, quote_via=quote_plus))

def transcript_explore_link(audio_key, t=0, text="Link"):
    url = transcript_explore_url(audio_key=audio_key, t=t)

    return '<a href="{0}">{1}</a>'.format(url, text)

##
## Cosine similarity utility functions
##

def sparse_normalize(m, zeros='ignore'):
    assert zeros in ('drop', 'raise', 'ignore')

    n = ss.linalg.norm(m, axis=1) # raises if input is not sparse

    # Rows with zero norm are the zero vector (all entries zero).
    # We can't normalize these because we can't divide by zero.
    # Sensible options are to drop these rows or raise.
    if zeros == 'raise':
        if (n == 0).sum() > 0:
            raise ValueError("Input contains zero-norm rows")
    elif zeros == 'drop':
        m = m[n != 0, :]
        n = n[n != 0]
    else: # zeros == 'ignore'
        pass

    # Just using the "/" operator densifies the result
    with np.errstate(divide='ignore', invalid='ignore'):
        val = np.repeat(1.0/n, m.getnnz(axis=1))
        rn = ss.csr_matrix((val, m.nonzero()), shape=(m.shape))

    return m.multiply(rn)

def dense_normalize(m, zeros='ignore'):
    assert zeros in ('drop', 'raise', 'ignore')

    n = np.linalg.norm(m, axis=1)

    if zeros == 'raise':
        if (n == 0).sum() > 0:
            raise ValueError("Input contains zero-norm rows")
    elif zeros == 'drop':
        m = m[n != 0, :]
        n = n[n != 0]
    else: # zeros == 'ignore'
        pass

    with np.errstate(divide='ignore', invalid='ignore'):
        ret = m / n[:, np.newaxis]

    return ret

def normalize(m):
    if ss.issparse(m):
        return sparse_normalize(m)
    else:
        return dense_normalize(m)

def matmul(m1, m2, norm=False):
    if norm:
        m1, m2 = normalize(m1), normalize(m2)

    if ss.issparse(m1) and ss.issparse(m2):
        ret = m1 * m2
    else:
        # If only one is sparse, treat both as dense
        if ss.issparse(m1):
            m1 = m1.toarray()
        if ss.issparse(m2):
            m2 = m2.toarray()

        ret = np.matmul(m1, m2)

    return ret

def nanmean(m1):
    if ss.issparse(m1):
        return np.nanmean(m1.data)
    else:
        return np.nanmean(m1)

##
## Similarity evaluation utility functions
##

def topk_1d(x, k=1):
    std = np.argsort(x.ravel())
    std = std[~np.isnan(x[std])]

    return std[-k:]

def topk_array(a, k=1):
    a = np.squeeze(np.asarray(a))

    return np.apply_along_axis(lambda x: topk_1d(x, k=k), 0, a)

def topk(vecs, which, k=1, dist='cosine', norm=False):
    assert dist in ('cosine', 'euclidean')

    if dist == 'cosine':
        # cosine similarity closer to 1 => more similar
        # (1 minus this is the actual cosine distance, but
        # the top k of -(1-mat) = mat - 1 is the same as
        # the top k of mat)
        mat = matmul(vecs[which, :], vecs.T, norm=norm).T

        if ss.issparse(mat):
            mat = mat.todense()

        # every vector will match itself perfectly, so let's
        # avoid returning them (-1 is the lowest possible
        # cosine similarity)
        for i, w in enumerate(which):
            mat[w, i] = -1
    else:
        if ss.issparse(vecs):
            raise NotImplementedError()
        else:
            mat = np.apply_along_axis(lambda x: np.linalg.norm(vecs - x, axis=1),
                                      1, vecs[which, :]).T

            # similarly, make self-matches infinitely far apart
            for i, w in enumerate(which):
                mat[w, i] = np.inf

            # Note the "-": need the k rows with *lowest* distance, unlike
            # with the cosine similarity above
            mat = -mat

    indices = topk_array(mat, k=k)

    if len(indices.shape) == 1:
        indices = np.expand_dims(indices, 1)

    values = []
    for r in range(0, indices.T.shape[0]):
        values += [[
            mat[v, r]
            for v in indices.T[r, :]
        ]]
    values = np.vstack(values)

    return (indices, values)

##
## Misc utils
##

class DenseTransformer(sk.base.TransformerMixin):
    def fit(self, X, y=None, **fit_params):
        return self

    def transform(self, X, y=None, **fit_params):
        return X.todense()

# Should be used only as a context manager
class DBCorpus(object):
    """
    A database-backed iterable for large resultsets, via
    scrollable server-side cursors.
    """

    def __init__(self, query, curname='dbcorpus_cursor',
                 conn_args={'host': '/var/run/postgresql'}):
        self.query = query
        self.curname = curname
        self.conn_args = conn_args

    def __enter__(self):
        self.conn = psycopg2.connect(**self.conn_args)

        # named server-side cursor to support scrolling
        self.cur = self.conn.cursor(name=self.curname, scrollable=True,
                                    cursor_factory=psycopg2.extras.DictCursor)
        self.cur.itersize=20

        self.cur.execute(self.query)

        return self

    def __exit__(self, type, value, tb):
        self.cur.close()
        self.conn.close()

        delattr(self, 'cur')
        delattr(self, 'conn')

    def __iter__(self):
        return self

    def __next__(self):
        ret = self.cur.fetchone()

        if ret is not None:
            return ret
        else:
            self.cur.scroll(0, mode='absolute')
            raise StopIteration()

def coalesce(*args):
    try:
        return next(filter(lambda x: x is not None, args))
    except StopIteration:
        return None

def grouper(it, n=None):
    assert n is None or n > 0

    if n is None:
        yield [x for x in it]
    else:
        ret = []

        for obj in it:
            if len(ret) == n:
                yield ret
                ret = []

            if len(ret) < n:
                ret += [obj]

        # at this point, we're out of
        # objects but len(ret) < n
        if len(ret) > 0:
            yield ret

def daterange(start, end):
    # the generator version
    # for n in range(int ((end - start).days)):
    #     yield start + dt.timedelta(n)

    # the materializing version which plays better with tqdm
    return [
        start + dt.timedelta(n)
        for n in range(int ((end - start).days))
    ]

##
## Multiprocessing helpers
##

def _payload(func, *files):
    args = []

    for fn in files:
        with open(fn, 'rb') as f:
            args += [pickle.load(f)]

    return func(*args)

def do_for_subsets(data, subsets, which, func, processes='cpu_count',
                   progress=None, skip_errors=False):
    if progress is not None:
        prg = progress(total=len(which))

    nprocs = os.cpu_count() if processes == 'cpu_count' else processes

    with contextlib.ExitStack() as stack:
        caches = {}
        for s in subsets.keys():
            f = stack.enter_context(tempfile.NamedTemporaryFile())
            caches[s] = f.name

            if isinstance(data, np.ndarray):
                pickle.dump(data[subsets[s], :], f)
            elif isinstance(data, pd.DataFrame):
                pickle.dump(data.loc[subsets[s], :], f)
            else:
                msg = "Don't know how to handle {0} object".format(type(data))
                raise ValueError(msg)

            # make the data available to forked processes
            f.flush()
            os.fsync(f.fileno())

        with mp.Pool(processes=nprocs) as pool:
            results = [
                pool.apply_async(_payload, [func] + [caches[s] for s in target])
                for target in which
            ]

            pool.close()

            ret = {}
            failed = []
            while len(ret.keys()) + len(failed) < len(which):
                for target, res in zip(which, results):
                    if tuple(target) in failed:
                        continue

                    if tuple(target) in ret.keys():
                        continue

                    if res.ready():
                        try:
                            ret[tuple(target)] = res.get()
                        except Exception:
                            failed += [tuple(target)]

                            if skip_errors == 'log':
                                logger.exception('Failed computation')
                            elif skip_errors:
                                pass
                            else:
                                raise

                        if progress is not None:
                            prg.update(1)
                            prg.refresh()

            pool.join() # final cleanup

    return ret

def parallel_by(data, func, grpvar='group', progress=None):
    masks = {
        grp : data[grpvar] == grp
        for grp in data[grpvar].unique()
    }

    targets = [(x, x) for x in masks.keys()]

    tmp = do_for_subsets(data, masks, targets, func, progress=progress)

    return [
        [g1, result]
        for (g1, g2), result in tmp.items() # g1 == g2
    ]

##
## File manipulation utils
##

# Just like the unix command
def rm_rf(path):
    pth = pl.Path(path)
    ab = str(pth.absolute())

    if pth.is_dir() and not pth.is_symlink():
        shutil.rmtree(ab)
    elif pth.exists():
        os.remove(ab)

# Smarter opening of files
def gzip_safe_open(f, mode='rt'):
    if f.lower().endswith('.gz'):
        func = gzip.open
    else:
        func = open

    return func(f, mode)

@contextlib.contextmanager
def smart_open(filename='-', mode='r', method=gzip_safe_open):
    is_read = (mode[0] == 'r')

    if filename and filename != '-':
        fh = method(filename, mode)
    elif is_read:
        fh = sys.stdin
    else:
        # it doesn't make sense to ask for read on stdout or write on
        # stdin, so '-' can be unambiguously resolved to one or the other
        # depending on the open mode
        fh = sys.stdout

    try:
        yield fh
    finally:
        if is_read and fh is not sys.stdin:
            fh.close()
        elif not is_read and fh is not sys.stdout:
            fh.close()

# Get all non-directory files under a path
def _files_under(path, absolute=True):
    pth = pl.Path(path)

    if not pth.is_dir():
        files = [pth]
    else:
        files = [x for x in pth.glob('**/*') if not x.is_dir()]
        files = sorted(files)

    if absolute:
        return [str(x.absolute()) for x in files]
    else:
        return [str(x) for x in files]

def files_under(*paths, absolute=True, dedupe=True):
    files = [_files_under(x, absolute=absolute) for x in paths]
    files = [x for y in files for x in y] # flatten it

    if dedupe:
        seen = set()
        return [x for x in files if x not in seen and not seen.add(x)]

    return files

# Do something embodied by a callable for all files under a path. If the path is
# itself a file, rather than a directory, do the callable for the path; if it's
# a directory, recursively act on each file in it.
def walk_files(paths, func, dedupe=True, args=[], kwargs={}):
    for f in files_under(paths, dedupe=dedupe):
        func(f, *args, **kwargs)

# Given a file or directory (input) and another path (prefix), get all files
# under the input and return a) them, b) them expressed relative to the prefix
def relativize(input_file, output_file, mkdir=True):
    sources = files_under(str(input_file))

    if not input_file.is_dir():
        # source is a file => len(sources) == 1 and target is a file
        targets = [str(output_file.absolute())]
    else:
        # source is a dir => len(sources) >= 1 and target is a dir
        if mkdir:
            output_file.mkdir(parents=True, exist_ok=True)

        targets = [pl.Path(x) for x in sources]
        targets = [x.relative_to(input_file.absolute()) for x in targets]
        targets = [(output_file / x).absolute() for x in targets]

    return sources, targets

##
## Phrase-combining functions
##

# Similar to the implementation in gensim but lets us
# specify our own phrases - no model, no detection, just
# the mechanics of combining n-grams in a prespecified list.
def analyze_sentence(s, phrases, dedupe_phrases=True, common_terms=[]):
    if dedupe_phrases:
        p = set([tuple(x) for x in phrases])
    else:
        p = phrases

    s.append(None)
    last_uncommon = None
    in_between = []

    for word in s:
        is_common = word in common_terms
        if not is_common and last_uncommon:
            chain = [last_uncommon] + in_between + [word]

            if tuple(chain) in p:
                yield (chain, True)
                last_uncommon = None
                in_between = []
            else:
                # release words individually
                for w in it.chain([last_uncommon], in_between):
                    yield (w, False)
                in_between = []
                last_uncommon = word
        elif not is_common:
            last_uncommon = word
        else:  # common term
            if last_uncommon:
                # wait for uncommon resolution
                in_between.append(word)
            else:
                yield (word, False)

def phrase(s, phrases, delim='_', dedupe_phrases=True, common_terms=None):
    if common_terms is None:
        common_terms = []
    else:
        # assumed to be a language name
        common_terms = cp.stopwords.words(common_terms)

    ret = []
    for w, flag in analyze_sentence(s, phrases, dedupe_phrases, common_terms):
        w = delim.join(w) if flag else w
        ret.append(w)

    return ret

##
## Some misc utilities for notebook use
##

def split_oov_compounds(sentence, model):
    ret, sentence = [], sentence.split(' ')

    for s in sentence:
        if '_' in s and not model.vocab.has_vector(s):
            ret += s.split('_')
        else:
            ret += [s]

    return ' '.join(ret)

def to_vectors(sentences, model, a=1e-3, split_oov=True, progress=lambda x: x):
    ##
    ## We've done collocation detection on the radio data, so
    ## if requested remove collocations not in model vocabulary
    ##

    if split_oov:
        sentences = sentences.apply(lambda x: split_oov_compounds(x, model))

    ##
    ## Unigram probabilities of the vocabulary for use in weights below
    ##

    probs = sentences.str.split(' ').iteritems()
    probs = cl.Counter(iterflatten(iterfield(probs, 1)))
    probs = {k : v / sum(probs.values()) for k, v in probs.items()}

    ##
    ## Weighted average of each sentence's word vectors
    ##

    ret = []
    for sentence in progress(sentences):
        doc = model.tokenizer(sentence)
        tokens = [x.text for x in doc]
        vectors = [x.vector for x in doc]

        weights = [a / (a + probs.get(w, 0)) for w in tokens]

        vec = [x * y for x, y in zip(weights, vectors)]
        vec = np.stack(vec, axis=0).mean(axis=0)

        ret += [vec]
    ret = np.stack(ret, axis=0)

    ##
    ## Remove the "common component" (projection onto first singular vector)
    ##

    mod = TruncatedSVD(n_components=1, algorithm='arpack').fit(ret)

    sv = mod.components_[0]
    proj = np.outer(sv, sv)

    ret = np.apply_along_axis(lambda x: x - np.inner(proj, x), 1, ret)

    return ret

_english_stopwords = set(cp.stopwords.words('english'))
def approxhash(line, stopwords=_english_stopwords):
    line = [x.lower().strip() for x in line if x not in stopwords]
    line = ' '.join(line).encode('utf-8')

    return base64.b64encode(hashlib.md5(line).digest()).decode('utf-8')

##
## Various iterator interfaces
##

def iterflatten(iterable):
    for s in iterable:
        yield from s

def itersample(iterable, frac=0.01, batchsize=20000):
    for s in grouper(iterable, batchsize):
        yield from random.sample(s, int(frac * len(s)))

# Like itertools' chain and cycle, but cycling without auxiliary storage.
# Assumes the component iterables offer restartable iteration. If they're
# actually exhausted at the end of the first iteration and can't be reused,
# like open file objects, this won't work.
class cyclechain(object):
    def __init__(self, *iterables):
        self._iterables = iterables

    def __iter__(self):
        return it.chain(*self._iterables)

class iterfield(object):
    def __init__(self, iterable, field=0):
        self._iterable = iterable
        self._field = field

        self._iterator = iter(self._iterable)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            item = next(self._iterator)
        except StopIteration:
            self._iterator = iter(self._iterable)

            raise StopIteration

        return item[self._field]

def iterjoin(iterable, sep=' ', skipnone=False):
    for s in iterable:
        if s is None and skipnone:
            continue

        yield sep.join(s)

