#!/usr/bin/env python3

"""
Given a radiosearch snippet and a query string, write out an mp3 of a window of audio
around the query.
"""

import os
import shutil
import tempfile

from .file_conversion import get_audio_subsegment, convert_to_mp3
from . import radiosearch_lib
from .radiosearch_lib import BUCKET_NAME, SPEECH_TMP_DIRECTORY


def save_audio_clip(
    segment_data,
    query,
    s3_resource,
    output_dir,
    reco_result,
    time_window_left=10,
    time_window_right=20,
):
    audio_key = segment_data["audio_key"]
    word_tuples = reco_result["words"]  # [(word, start, end, ...), ...]
    start_time = segment_data["segment_start_relative"]

    query_words = query.split()
    for i in range(len(word_tuples)):
        if word_tuples[i][1] < start_time:
            # Advance to start time of segment
            continue
        seq = [x[0] for x in word_tuples[i : i + len(query_words)]]
        if seq == query_words:
            start_time = max(float(word_tuples[i][1]) - time_window_left, 0)
            # ok to go over
            end_time = (
                float(word_tuples[i + len(query_words) - 1][2]) + time_window_right
            )
            output_path = os.path.join(output_dir, segment_data["audio_path"])
            if not os.path.isdir(os.path.dirname(output_path)):
                os.makedirs(os.path.dirname(output_path))
            tmp_directory = tempfile.mkdtemp(prefix=SPEECH_TMP_DIRECTORY)
            tmp_file = tempfile.NamedTemporaryFile(
                mode="w", prefix=tmp_directory + "/", delete=True
            )

            # Get full clip audio
            s3_resource.Bucket(BUCKET_NAME).download_file(audio_key, tmp_file.name)
            new_fname = file_conversion.convert_to_mp3(tmp_file.name)

            # Snip
            file_conversion.get_audio_subsegment(
                new_fname, output_path, start_time, end_time
            )
            # We're done with this instance
            shutil.rmtree(tmp_directory)
            return

    # Couldn't find match, uh oh!
    print("Couldn't find match for query %s in %s", query, str(segment_data))
