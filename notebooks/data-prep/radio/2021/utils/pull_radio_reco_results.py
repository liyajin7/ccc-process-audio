#!/usr/bin/env python3

"""
This script pulls speech recognition result files from S3 and, by
default, postprocesses them into a tab-separated value (TSV) file of
"snippets" suitable for loading directly into a postgres database.
See ./reindex.sh for the script that controls this.  This script can
also be used to run a variety of other data pulls of the recognition
result files.  Run the command with no arguments for more info.

It's expected that CACHE_DIR exists locally when you run this.  This
directory is used to cache recognition result files between multiple
runs of this script, since copying them from s3 each time would be
very slow.
"""

import argparse
import concurrent
from datetime import datetime, timedelta
import json
import logging
import multiprocessing as mp
import os
import sys

# cortico contains code extracted from git@github.com:mit-ccc/cortico-code-dump
from cortico import audio_clipper
from cortico import radiosearch_lib
from cortico import station_metadata
from cortico import word_metadata

CACHE_DIR = './tmp'

logger = logging.getLogger(__name__)

def output_snippet(
    segment_data, station_metadata_obj, output_json=False, word_metadata_obj=None
):
    """Given the raw data for a segment in a dictionary, output a TSV row
    to be used for import into the database.
    """
    callsign = radiosearch_lib.audio_key_to_station(segment_data["audio_key"])
    station_info = station_metadata_obj.get_station_info(callsign)
    city = station_info and station_info.get("city") or ""
    state = station_info and station_info.get("state") or ""
    if output_json:
        segment_data["callsign"] = callsign
        segment_data["city"] = city
        segment_data["state"] = state
        show_info = station_metadata_obj.get_show_for_station_and_time(
            callsign, datetime.fromtimestamp(segment_data["segment_start_global"])
        )
        if show_info:
            segment_data["show_name"] = show_info[0]
            segment_data["show_source"] = show_info[1]
            segment_data["show_confidence"] = show_info[2]
        if word_metadata_obj:
            segment_data["denorm_content"] = word_metadata_obj.denorm_text(
                segment_data["content"]
            )
        print(json.dumps(segment_data))
    else:
        key = segment_data["audio_key"]
        segment_idx = segment_data["segment_idx"]
        content = segment_data["content"]
        segment_start_global = segment_data["segment_start_global"]
        segment_start_relative = segment_data["segment_start_relative"]

        # Diarization fields added 2018-05-25:
        diary_speaker_id = segment_data.get("diary_speaker_id", "_")
        diary_gender = segment_data.get("diary_gender", "_")
        diary_env = segment_data.get("diary_env", "_")
        diary_band = segment_data.get("diary_band", "_")

        # Confidence score added 2018-11-07:
        mean_word_confidence = segment_data.get("mean_word_confidence", 0.0)

        print(
            "%s\t%s\t%s\t%f\t%f\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%f"
            % (
                key,
                content,
                segment_idx,
                segment_start_global,
                segment_start_relative,
                callsign,
                city,
                state,
                diary_speaker_id,
                diary_gender,
                diary_env,
                diary_band,
                mean_word_confidence,
            )
        )


def fill_cache_thread(date, s3_resource):
    """A single worker for the fill_cache method below."""
    for x in generate_result_objects(
        date, 1, s3_resource, write_to_cache=True, cache_only=True
    ):
        # Do nothing with the files, we're just filling the cache.
        pass


def fill_cache(start_date, num_days, s3_resource):
    """Fill the local disk cache of recognition result files for the given time range
    by running many days in parallel;  the workers are i/o bound."""
    thread_pool = concurrent.futures.ThreadPoolExecutor(25)
    for x in range(num_days):
        this_date = start_date - timedelta(days=x)
        thread_pool.submit(fill_cache_thread, this_date, s3_resource)
    thread_pool.shutdown(wait=True)


def generate_result_objects(
    start_date,
    num_days,
    s3_resource,
    write_to_cache=True,
    cache_only=False,
    logger=None,
):
    """A generator that yields a result object for all of the radio recognition results
    in the past num_days in reverse chronological order."""
    s3_client = s3_resource.meta.client
    paginator = s3_client.get_paginator("list_objects")

    for x in range(num_days):
        date = (start_date - timedelta(days=x)).strftime("%Y-%m-%d")
        if logger:
            logger.info("Processing files for %s...", date)
        page_iterator = paginator.paginate(
            Bucket=radiosearch_lib.BUCKET_NAME,
            PaginationConfig={"PageSize": 1000},
            Prefix=radiosearch_lib.S3_PREFIX + date + "/",
        )
        num_files = 0
        if write_to_cache:
            try:
                os.mkdir(CACHE_DIR.replace("{DATE}", date))
            except OSError:
                pass
        for page in page_iterator:
            if not page or "Contents" not in page:
                continue
            for item in page["Contents"]:
                key = item["Key"]
                if key.endswith(radiosearch_lib.SEGMENTS_SUFFIX) or key.endswith(
                    radiosearch_lib.RESULTS_SUFFIX
                ):
                    cache_fname = os.path.join(
                        CACHE_DIR.replace("{DATE}", date), key.replace("/", "_")
                    )
                    try:
                        with open(cache_fname) as f:
                            content = f.read()
                    except FileNotFoundError:
                        obj = s3_resource.Object(radiosearch_lib.BUCKET_NAME, key)
                        if obj.storage_class == "GLACIER":
                            if obj.restore is None:
                                if logger:
                                    logger.info("Making restore request for %s", key)
                                restore_request = {"Days": 30}
                                s3_client.restore_object(
                                    Bucket=radiosearch_lib.BUCKET_NAME,
                                    Key=key,
                                    RestoreRequest=restore_request,
                                )
                                continue
                            elif 'ongoing-request="true"' in obj.restore:
                                if logger:
                                    logger.info("Restoration in progress for %s", key)
                                continue
                            elif 'ongoing-request="false"' in obj.restore:
                                if logger:
                                    logger.debug("Restoration is done for %s", key)

                        content = obj.get()["Body"].read().decode("utf-8")
                        if write_to_cache:
                            with open(cache_fname, "w") as f:
                                f.write(content)
                    num_files += 1
                    if cache_only:
                        yield key
                    elif content:
                        audio_key = key.replace(
                            radiosearch_lib.SEGMENTS_SUFFIX, ""
                        ).replace(radiosearch_lib.RESULTS_SUFFIX, "")
                        # Pre-5/22 these were TSV files with just the words array
                        if key.endswith(radiosearch_lib.SEGMENTS_SUFFIX):
                            yield audio_key, {
                                "words": radiosearch_lib.parse_tsv(content)
                            }
                        else:
                            try:
                                yield audio_key, json.loads(content)
                            except json.decoder.JSONDecodeError:
                                print("Bad line: %s" % (content), file=sys.stderr)
                                continue

        if logger:
            logger.info("...%d files processed for %s", num_files, date)


def segment_matches_query(segment_data, query):
    """Does the content of this result match the given search query?  If there's a comma
    in the query string, treat it as an OR of the comma-separated parts."""
    s = " " + segment_data["content"].lower() + " "
    if query.find(",") != -1:
        for q in query.split(","):
            if s.find(" " + q + " ") != -1:
                return True
        return False
    return s.find(" " + query + " ") != -1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--json",
        default=False,
        action="store_true",
        help="Write output in JSON format instead of Postgres import TSV",
    )
    parser.add_argument(
        "--nocache",
        default=False,
        action="store_true",
        help="Don't cache results files from s3 to local disk",
    )
    parser.add_argument(
        "--write_audio",
        default=False,
        action="store_true",
        help="Write the audio files for the matching snippets.",
    )
    parser.add_argument(
        "--audio_output_dir", help="Output directory for --write_audio", default="/tmp"
    )
    parser.add_argument("--query", help="Only output snippets that match this query")
    parser.add_argument(
        "--fill_cache",
        default=False,
        action="store_true",
        help="Just populate the result files cache, don't output anything.",
    )
    parser.add_argument("--num_days", type=int, help="Go back this many days")
    parser.add_argument(
        "--start_date", help="Start date in YYYY-MM-DD format; default is today"
    )
    args = parser.parse_args()

    fmt = '%(asctime)s : %(levelname)s : %(message)s'
    logging.basicConfig(format=fmt, level=logging.INFO)

    num_days = args.num_days or radiosearch_lib.SEARCH_INDEX_NUM_DAYS
    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    else:
        start_date = datetime.now()
    station_metadata_obj = station_metadata.StationMetadata()
    word_metadata_obj = word_metadata.WordMetadata()
    s3_resource = radiosearch_lib.get_s3()
    if args.fill_cache:
        fill_cache(start_date, num_days, s3_resource)
        sys.exit()
    if args.write_audio:
        thread_pool = concurrent.futures.ThreadPoolExecutor(4)

    # Keep stats on station,date counts for analysis
    station_date_word_counts = {}

    snippet_num = 0
    for audio_key, content in generate_result_objects(
        start_date, num_days, s3_resource,
        write_to_cache=not args.nocache,
        logger=logger,
    ):
        station = radiosearch_lib.audio_key_to_station(audio_key)
        segments = radiosearch_lib.reco_results_to_segments(content, audio_key)
        for segment_data in segments:
            datestring = radiosearch_lib.audio_key_to_datestring(audio_key)
            if (
                (
                    len(segment_data["content"])
                    >= radiosearch_lib.MIN_OUTPUT_SENTENCE_LENGTH
                )
                or args.json
            ) and (not args.query or segment_matches_query(segment_data, args.query)):
                if args.write_audio:
                    audio_path = "%s/%d.mp3" % (datestring, snippet_num)
                    if not os.path.isfile(
                        os.path.join(args.audio_output_dir, audio_path)
                    ):
                        segment_data["audio_path"] = audio_path
                        thread_pool.submit(
                            audio_clipper.save_audio_clip,
                            segment_data,
                            args.query,
                            s3_resource,
                            args.audio_output_dir,
                            content,
                        )
                output_snippet(
                    segment_data,
                    station_metadata_obj,
                    output_json=args.json,
                    word_metadata_obj=word_metadata_obj,
                )
                snippet_num += 1
            station_date = station + "," + datestring
            station_date_word_counts[station_date] = station_date_word_counts.get(
                station_date, 0
            ) + len(segment_data["content"].split())

    if args.write_audio:
        thread_pool.shutdown(wait=True)
