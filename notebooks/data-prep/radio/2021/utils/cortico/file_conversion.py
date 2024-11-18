#!/usr/bin/python3

"""
Utilities for dealing with audio files.
"""

import logging
import subprocess

AVCONF_CMD = "/usr/bin/ffmpeg -i %s -ac 1 %s"
FILE_CMD = "/usr/bin/file %s"

FFMPEG_CMD = "/usr/bin/ffmpeg"

# is %f here correct for seconds?  or is the stuff after . msec
SUBSEGMENT_CMD = FFMPEG_CMD + " -y -ss %f -i %s -acodec copy -t %f %s"

# Convert to mp3 and subsegment in one call
SUBSEGMENT2_CMD = FFMPEG_CMD + " -y -i %s -ac 1 -ss %f -t %f %s"


def audio_file_type(fname):
    x = subprocess.check_output((FILE_CMD % (fname)).split())
    if x.find(b"AAC,") != -1:
        return "AAC"
    elif x.find(b"WAVE,") != -1:
        if x.find(b" mono ") != -1:
            return "WAV_MONO"
        else:
            return "WAV_STEREO"
    return "OTHER"


def convert_to_wav(fname):
    x = subprocess.check_output((AVCONF_CMD % (fname, fname + ".wav")).split())
    return fname + ".wav"


def convert_to_mp3(fname):
    x = subprocess.check_output((AVCONF_CMD % (fname, fname + ".mp3")).split())
    return fname + ".mp3"


def get_audio_subsegment(fname, out_fname, start_secs, end_secs):
    duration = end_secs - start_secs
    x = subprocess.check_output(
        (SUBSEGMENT_CMD % (start_secs, fname, duration, out_fname)).split()
    )
    return x


def get_mp3_subsegment(fname, out_fname, start_secs, end_secs):
    duration = end_secs - start_secs
    x = subprocess.check_output(
        (SUBSEGMENT2_CMD % (fname, start_secs, duration, out_fname)).split()
    )
    return x


def concatenate_files(file_list, out_fname):
    # https://superuser.com/questions/587511/concatenate-multiple-wav-files-using-single-command-without-extra-file
    concat_cmd = FFMPEG_CMD + " " + " ".join(["-i %s" % (fname) for fname in file_list])
    concat_cmd += " -filter_complex %sconcat=n=%d:v=0:a=1[out] -map [out] %s" % (
        "".join(["[%d:0]" % (x) for x in range(len(file_list))]),
        len(file_list),
        out_fname,
    )
    try:
        x = subprocess.check_output(concat_cmd.split())
    except subprocess.CalledProcessError:
        print("Concatenation error", concat_cmd)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    # audio_file_type("15_44_35_temp.wav")
    convert_to_wav("15_44_35_temp.aac")
