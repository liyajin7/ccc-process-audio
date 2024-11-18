from typing import Dict, Any, Union, Optional

import os
import io
import json
import time
import logging
import subprocess
import multiprocessing as mp
from abc import ABC, abstractmethod
from functools import cached_property
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd

import boto3
import botocore

import ffmpeg
from faster_whisper import WhisperModel

import torch

from IPython.display import Audio

# will switch automatically to the console version when on console
from tqdm import tqdm


logger = logging.getLogger(__name__)


def get_free_gpus(min_memory_mb=1024):
    result = subprocess.run([
        'nvidia-smi',
        '--query-gpu=memory.free',
        '--format=csv'
    ], stdout=subprocess.PIPE)
    # command line output looks like:
    # $ nvidia-smi --query-gpu=memory.free --format=csv
    # memory.free [MiB]
    # 11171 MiB
    # 11171 MiB
    # 11171 MiB
    # 11171 MiB

    result = result.stdout.decode('utf-8')
    result = result.strip().split('\n')[1:]  # drop header

    # Loop through each line and extract the GPU index and free memory
    free_memory = {}
    for i, line in enumerate(result):
        index, memory = i, int(line.strip().split()[0])
        if memory >= min_memory_mb:  # 1GB in megabytes
            free_memory[index] = memory

    return free_memory


#
# Playing audio for debugging; use in a jupyter notebook
#

def play_s3_audio(bucket: str, key: str, start_time: float,
                  duration: float, aws_profile: str):
    session = boto3.Session(profile_name=aws_profile)
    s3 = session.client('s3')

    # Download the file into a BytesIO object
    input_buffer = io.BytesIO()
    s3.download_fileobj(bucket, key, input_buffer)
    input_buffer.seek(0)

    out = _load_audio(
        input_buffer,
        start_time=start_time,
        end_time=start_time+duration,
        sr=16000,
    )

    return Audio(out, rate=16000) if out.size > 0 else None


#
# Loading audio
#

def _load_audio_fobj(audio, sr: int = 16000, start_time: Union[float, int] = 0,
                     end_time: Optional[Union[float, int]] = None):
    try:
        audio.seek(0, 0)

        input_data = b''
        while True:
            chunk = audio.read(4096)
            if not chunk:
                break
            input_data += chunk

        out, _ = (
            ffmpeg.input(
                'pipe:',
                threads=0,
                # ss=start_time,
                seek_timestamp=1,
                err_detect='ignore_err',
                # **({"to": end_time} if end_time else {})
            )
            .output("-", format="s16le", acodec="pcm_s16le", ac=1, ar=sr)
            .run(
                input=input_data,
                cmd=["ffmpeg", "-nostdin"],
                capture_stdout=True,
                capture_stderr=True
            )
        )
    except ffmpeg.Error as e:
        raise RuntimeError(f"Failed to load audio: {e.stderr.decode()}") from e

    ret = np.frombuffer(out, np.int16).flatten().astype(np.float32) / 32768.0

    ss, es = int(sr*start_time), int(sr*(end_time if end_time else ret.shape[0]))
    return ret[ss:es]

def _load_audio(audio, **kwargs: Any):
    if not isinstance(audio, str):
        return _load_audio_fobj(audio, **kwargs)
    else:
        with open(audio, 'rb') as f:
            return _load_audio_fobj(f, **kwargs)


#
# Reformatting whisper output
#

def segment_to_json(segment):
    segment_fields = ['start', 'end', 'text', 'avg_log_prob', 'no_speech_prob']
    word_fields = ['start', 'end', 'word', 'probability']

    ret = {}
    for field in segment_fields:
        ret[field] = getattr(segment, field)

    ret['words'] = []
    for word in getattr(segment, 'words'):
        ret['words'] += [{f: getattr(word, f) for f in word_fields}]

    return ret


def whisper_to_json(segments, info):
    segments = [segment_to_json(seg) for seg in segments]
    return dict(info, segments=segments)


#
# One S3 audio file; encapsulates processing all associated
# snippets at once
#

class FileTask:
    def __init__(self, bucket, key, sections, client):
        super().__init__()

        self.bucket = bucket
        self.key = key
        self.sections = sections
        self.client = client

    def _fetch(self):
        resp = self.client.get_object(
            Bucket=self.bucket,
            Key=self.key,
        )

        return io.BytesIO(resp['Body'].read())

    def __len__(self):
        return self.sections.shape[0]

    def __iter__(self):
        with self._fetch() as _audio:
            for section in self.sections.itertuples():
                start = getattr(section, 'offset')
                end = start + getattr(section, 'duration')

                yield {
                    'id': getattr(section, 'id'),
                    'audio': _load_audio(_audio, start_time=start, end_time=end)
                }


#
# Transcriber classes - base, CPU, multi-CPU, GPU, CPU/GPU combined
#

class Transcriber(ABC):
    def __init__(self, bucket: str, tasks: Dict[str, pd.DataFrame],
                 aws_profile: str, model: Any,
                 cache_dir: str = None, cache_only: bool = False,
                 check_cache_on_start: bool = True, progress: bool = True):
        super().__init__()

        if not cache_dir and cache_only:
            raise ValueError('Must specify cache_dir for cache_only')

        self.bucket = bucket
        self.tasks = tasks
        self.aws_profile = aws_profile
        self.model = model
        self.cache_dir = cache_dir
        self.cache_only = cache_only
        self.check_cache_on_start = check_cache_on_start
        self.progress = progress

        if self.cache_dir:
            os.makedirs(self.cache_dir, exist_ok=True)

        self.session = boto3.Session(profile_name=self.aws_profile)
        self.client = self.session.client('s3')

    def transcribe(self, key):
        if self.is_cached(key):
            return None if self.cache_only else self.read_from_cache(key)

        task = FileTask(
            bucket=self.bucket,
            key=key,
            sections=self.tasks[key],
            client=self.client,
        )

        ret = []
        for item in task:
            segs, info = self.model.transcribe(item['audio'], word_timestamps=True)
            info = dict(info._asdict(), id=item['id'])
            segs = list(segs)  # it's a generator; ASR happens here

            ret += [whisper_to_json(segs, info)]

        if self.cache_dir:
            self.write_to_cache(key, ret)

        return None if self.cache_only else ret

    @cached_property
    def start_cache(self):
        if not self.cache_dir:
            return None

        return {
            os.path.relpath(root, self.cache_dir)
            for root, dirs, _ in os.walk(self.cache_dir)
            if not dirs
        }

    def is_cached(self, key):
        if not self.cache_dir:
            return None

        if self.check_cache_on_start and self.start_cache is not None:
            if key in self.start_cache:
                return True

        key_path = os.path.join(self.cache_dir, key)
        return os.path.exists(key_path)

    def write_to_cache(self, key, res):
        assert self.cache_dir is not None

        key_path = os.path.join(self.cache_dir, key)
        os.makedirs(key_path, exist_ok=True)

        for item in res:
            item_path = os.path.join(key_path, f"{item['id']}.json")

            with open(item_path, 'wt') as f:
                json.dump(item, f)

    def read_from_cache(self, key):
        assert self.cache_dir is not None

        if not self.is_cached(key):
            raise RuntimeError(f'File {key} has not been processed yet')

        key_path = os.path.join(self.cache_dir, key)

        ret = []
        for obj in os.listdir(key_path):
            item_path = os.path.join(key_path, obj)

            with open(item_path, 'rt') as f:
                ret += [json.load(f)]

        return ret

    @abstractmethod
    def run(self):
        raise NotImplementedError()


class CudaTranscriber(Transcriber):
    def __init__(self, whisper_version='base', compute_type='auto',
                 cuda_devices='auto', **kwargs):
        if cuda_devices == 'auto':
            cuda_devices = list(get_free_gpus(min_memory_mb=1024).keys())
        if len(cuda_devices) == 0:
            raise RuntimeError('No free CUDA devices')

        self.num_workers = len(cuda_devices)

        kwargs['model'] = WhisperModel(
            whisper_version,
            device_index=cuda_devices,
            num_workers=self.num_workers,
            compute_type=compute_type,
        )

        super().__init__(**kwargs)

    def run(self, callback=None):
        keys = list(self.tasks.keys())
        np.random.shuffle(keys)  # representative timing estimates

        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = []
            with tqdm(
                total=len(self.tasks),
                desc='CUDA thread submit', unit='task',
                disable=(not self.progress)
            ) as pbar:
                for key in keys:
                    futures += [executor.submit(self.transcribe, key)]
                    pbar.update(1)

            ret = 0 if self.cache_only else []
            with tqdm(
                as_completed(futures),
                total=len(futures),
                desc='CUDA transcribe', unit='task',
                disable=(not self.progress),
            ) as pbar:
                for future in as_completed(futures):
                    try:
                        res = future.result()

                        ret += 1 if self.cache_only else [res]

                        pbar.update(1)
                        if callback:
                            callback()
                    except Exception as exc:
                        logger.exception('Unhandled exception in thread')

            return ret


class CpuTranscriber(Transcriber):
    def __init__(self, whisper_version='base', compute_type='auto', **kwargs):
        kwargs['model'] = WhisperModel(
            whisper_version,
            device='cpu',
            num_workers=1,
            compute_type=compute_type,
        )

        super().__init__(**kwargs)

    def run(self, callback=None):
        keys = list(self.tasks.keys())
        np.random.shuffle(keys)  # representative timing estimates

        ret = 0 if self.cache_only else []
        with tqdm(
            keys,
            desc='CPU transcribe', unit='task',
            disable=(not self.progress),
        ) as pbar:
            for key in keys:
                try:
                    res = self.transcribe(key)

                    ret += 1 if self.cache_only else [res]

                    pbar.update(1)
                    if callback:
                        callback()
                except Exception as exc:
                    logger.exception('Unhandled exception in transcribe')

        return ret


def initializer(cnt):
    global counter

    counter = cnt

def worker(batch):
    global counter

    try:
        # one pbar, not one per process
        batch['worker_kwargs']['progress'] = False

        if batch['worker_type'] == 'cpu':
            transcriber = CpuTranscriber(**batch['worker_kwargs'])
        elif batch['worker_type'] == 'gpu':
            transcriber = CudaTranscriber(**batch['worker_kwargs'])
        else:
            raise ValueError("Invalid worker_type")

        if counter is not None:
            def update_counter():
                with counter.get_lock():
                    counter.value += 1

            return transcriber.run(callback=update_counter)
        else:
            return transcriber.run()
    except Exception as exc:
        pass  # FIXME


class MultiTranscriber:
    def __init__(self, tasks, n_procs=None, cuda_devices='auto', gpu_share=0.5,
                 cache_only=False, check_cache_on_start=True, progress=True,
                 **kwargs):
        assert not (gpu_share == 0 and cuda_devices)

        super().__init__()

        self.tasks = tasks
        self.n_procs = n_procs if n_procs is not None else os.cpu_count()
        self.cuda_devices = cuda_devices
        self.gpu_share = gpu_share
        self.cache_only = cache_only
        self.check_cache_on_start = check_cache_on_start
        self.progress = progress

        self._kwargs = kwargs
        self._kwargs['check_cache_on_start'] = False  # we do this here

        if (
            self.check_cache_on_start and
            'cache_dir' in self._kwargs and
            self._kwargs['cache_dir'] is not None
        ):
            start_cache = {
                os.path.relpath(root, self._kwargs['cache_dir'])
                for root, dirs, _ in os.walk(self._kwargs['cache_dir'])
                if not dirs
            }

            self.tasks = {
                k: self.tasks[k]
                for k in self.tasks.keys()
                if k not in start_cache
            }

    @cached_property
    def chunked_keys(self):
        keys = list(self.tasks.keys())
        np.random.shuffle(keys)

        cpu_keys = keys[0:int(len(keys) * (1 - self.gpu_share))]
        gpu_keys = keys[int(len(keys) * (1 - self.gpu_share)):len(keys)]
        total = len(cpu_keys) + len(gpu_keys)
        assert total == len(keys)

        n_cpu_procs = self.n_procs - 1 if gpu_keys else self.n_procs
        n_gpu_procs = self.n_procs - n_cpu_procs  # 0 xor 1

        worker_types = ['cpu'] * n_cpu_procs + ['gpu'] * n_gpu_procs

        worker_key_groups = []
        if n_cpu_procs > 0:
            worker_key_groups += [
                c.tolist()
                for c in np.array_split(cpu_keys, n_cpu_procs)
            ]
        if n_gpu_procs > 0:
            worker_key_groups += [gpu_keys]
        assert sum([len(c) for c in worker_key_groups]) == len(keys)

        return list(zip(worker_types, worker_key_groups))

    def get_pbar(self):
        return tqdm(total=len(self.tasks.keys()), desc='CPU/GPU transcribe',
                    unit='task', disable=(not self.progress))

    def run(self):
        proc_args = []
        for wtype, wkeys in self.chunked_keys:
            worker_kwargs = dict(
                self._kwargs,
                progress=False,
                cache_only=self.cache_only,
                tasks={k: self.tasks[k] for k in wkeys},
            )

            if wtype == 'gpu':
                worker_kwargs['cuda_devices'] = self.cuda_devices

            proc_args += [{
                'worker_type': wtype,
                'worker_kwargs': worker_kwargs
            }]

        counter = mp.Value('i', 0) if self.progress else None
        pool_kwargs = {
            'processes': self.n_procs,
            'maxtasksperchild': 1,
            'initializer': initializer,
            'initargs': (counter,),
        }

        with mp.Pool(**pool_kwargs) as pool:
            with self.get_pbar() as pbar:
                results = [
                    pool.apply_async(worker, (batch,))
                    for batch in proc_args
                ]

                pool.close()

                if self.progress:
                    while not all(res.ready() for res in results):
                        with counter.get_lock():
                            pbar.n = counter.value
                            pbar.refresh()
                        time.sleep(1)

                pool.join()

                ret = 0 if self.cache_only else []
                for res in results:
                    tmp = res.get()
                    ret += tmp if self.cache_only else [tmp]
                return ret
