#!/usr/bin/env python3

import os
import sys
import json
import time
import random
import logging
import argparse

from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import botocore
from botocore.exceptions import ClientError

from tqdm import tqdm


logger = logging.getLogger(__name__)


RESTORE_REQUEST_PARAMS = {
    'Days': 30,  # no. of days to keep object in S3 One Zone-IA storage class
    'GlacierJobParameters': {
        'Tier': 'Bulk'
    }
}


def exponential_backoff_with_jitter(retries, base_delay=0.05, max_delay=2.0):
    sleep_time = min(max_delay, (2 ** retries) * base_delay)
    sleep_time *= 0.5 + random.uniform(0.5, 1.5)

    return sleep_time


def restore_object(bucket, key):
    retries = 0
    max_retries = 5

    while retries <= max_retries:
        try:
            client.restore_object(
                Bucket=bucket,
                Key=key,
                RestoreRequest=RESTORE_REQUEST_PARAMS
            )
            msg = 'success'
            break
        except ClientError as exc:
            if exc.response['Error']['Code'] == 'ObjectAlreadyRestored':
                msg = 'ObjectAlreadyRestored'
                break
            elif exc.response['Error']['Code'] == 'RestoreAlreadyInProgress':
                msg = 'RestoreAlreadyInProgress'
                break
            elif exc.response['Error']['Code'] == 'RequestLimitExceeded' and retries < max_retries:
                sleep_time = exponential_backoff_with_jitter(retries)
                time.sleep(sleep_time)
                retries += 1
            else:
                msg = str(sys.exc_info())
                break
        except Exception as exc:
            msg = str(sys.exc_info())
            break

    return {key: msg}


def restore_objects(bucket, keys, concurrency_level=2 * os.cpu_count(),
                    verbose=False):
    with ThreadPoolExecutor(max_workers=concurrency_level) as executor:
        futures = []
        for key in tqdm(keys, disable=(not verbose)):
            futures.append(executor.submit(restore_object, bucket, key))

        for future in tqdm(as_completed(futures)):
            try:
                yield future.result()
            except Exception as exc:
                logger.exception('Unhandled exception in thread')


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--bucket', default='cortico-data')
    parser.add_argument('-a', '--aws-profile', default='cortico')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-s', '--seed', default=2969591811, type=int)
    parser.add_argument('-c', '--concurrency', default=2 * os.cpu_count(), type=int)

    args = parser.parse_args()


def cli(bucket, keys=None, aws_profile='default', verbose=False,
        seed=2969591811, concurrency=(2*os.cpu_count())):
    args = vars(args)

    ## Set seeds
    random.seed(seed)

    ## Logging
    logging.basicConfig(
        format='%(asctime)s : %(levelname)s : %(message)s',
        level=logging.INFO if verbose else logging.WARNING,
    )

    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)

    # AWS setup
    session = boto3.Session(profile_name=args['aws_profile'])
    config = botocore.config.Config(max_pool_connections=args['concurrency'])
    client = session.client('s3', config=config)

    # read keys from stdin
    if keys is None:
        keys = [line.strip() for line in sys.stdin]

    # restore objects
    for result in restore_objects(args['bucket'], keys, args['concurrency'], args['verbose']):
        print(json.dumps(result))


if __name__ == '__main__':
    args = parser.parse_args()

    cli(**vars(args))
