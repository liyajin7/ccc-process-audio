#!/usr/bin/env python3

"""
Utility functions and constants common to multiple parts of the radiosearch
pipeline (recognition, indexing, API server)
"""

import datetime
import json
import re

import boto3
from .geo_lib import STATE_PREFIXES

REGION_NAME = "us-east-1"
BUCKET_NAME = "cortico-data"
SEGMENTS_SUFFIX = (
    ".segments.tsv"  # Deprecating as of 5/22; rolling into RESULTS_SUFFIX file
)
RESULTS_SUFFIX = ".results.json"
SPEECH_TMP_DIRECTORY = "/tmp/radiospeech_tmp/"
S3_PREFIX = "speechbox/stream_out/"
S3_PREFIX_MEDLEYS = "speechbox/medleys/"
MAX_OUTPUT_SENTENCE_LENGTH = 1000
MIN_OUTPUT_SENTENCE_LENGTH = 20
SEARCH_INDEX_NUM_DAYS = 18

# format used for JSON output. also for converting audio key to date string.
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

FACETABLE_COLUMNS = ["state_code", "callsign", "city", "diary_gender", "diary_band"]
CUSTOM_STATION_GROUPS = {
    "Southeast_U.S.": [
        "state_code=LA",
        "state_code=MS",
        "state_code=GA",
        "state_code=TN",
        "state_code=AR",
        "state_code=AL",
        "state_code=SC",
        "state_code=NC",
        "state_code=FL",
        "state_code=KY",
    ],
    "Mexican border area": [
        "callsign=KOGO",
        "callsign=KUAZ",
        "callsign=KAWC",
        "callsign=KTSM",
        "callsign=KEVT",
    ],
    "Rust_Belt": [
        "state_code=NY",
        "state_code=PA",
        "state_code=WV",
        "state_code=OH",
        "state_code=IN",
        "state_code=MI",
        "state_code=IL",
    ],
    "Boston_MA": [
        "callsign=WBZ",
        "callsign=WBUR",
        "callsign=WGBH",
        "callsign=WRKO",
        "callsign=WXKS",
    ],
    "Phoenix_AZ": [
        "callsign=KTAR",
        "callsign=KFYI",
        "callsign=KJZZ",
        "callsign=KKNT",
        "callsign=KFNX",
    ],
    "Mobile_AL": [
        "callsign=WAVH",
        "callsign=WNTM",
        "callsign=WBUV",
        "callsign=WXQW",
        "callsign=WHIL",
    ],
    "Nashville_TN": ["callsign=WPLN-FM", "callsign=WLAC", "callsign=WWTN"],
    "Billings_MT": ["callsign=KBUL", "callsign=KCHH", "callsign=KEMC"],
    "I-4_Corridor_FL": [
        "callsign=WFLA",
        "callsign=WHPT",
        "callsign=WUSF",
        "callsign=WHNZ",
        "callsign=WMFE",
        "callsign=WDBO",
        "callsign=WFLF",
        "callsign=WTKS",
    ],
    "Pittsburgh_PA": [
        "callsign=KDKA",
        "callsign=WESA",
        "callsign=WKHB",
        "callsign=WPGP",
        "callsign=WJAS",
    ],
    "Madison_WI": ["callsign=WIBA", "callsign=WFAW", "callsign=WHA", "callsign=WORT"],
    "Orange_County_CA": [
        "callsign=KFI",
        "callsign=KPCC",
        "callsign=KRLA",
        "callsign=KABC",
        "callsign=KOGO",
        "callsign=KEIB",
    ],
    "Rural_Northeast_IA": [
        "callsign=KXEL",
        "callsign=WMT",
        "callsign=KXIC",
        "callsign=WHO",
        "callsign=KGLO",
        "callsign=KUNI",
    ],
    "Houston_TX": [
        "callsign=KTRH",
        "callsign=KUHF",
        "callsign=KPRC",
        "callsign=KNTH",
        "callsign=KTSA",
        "callsign=KLVI",
        "callsign=KSEV",
    ],
    "Atlanta_GA": [
        "callsign=WSB",
        "callsign=WABE",
        "callsign=WYAY",
        "callsign=WRAS",
        "callsign=WAOK",
        "callsign=WGKA",
        "callsign=WGST",
    ],
    "Providence_RI": [
        "callsign=WPRO",
        "callsign=WELH",
        "callsign=WGBH",
        "callsign=WHJJ",
        "callsign=WNRI",
        "callsign=WEAN",
    ],
}


def station_group_to_where_clauses(station_group):
    """Given a station group identifier such as "state_code=GA" or "Southeast_U.S.", generate
    the WHERE clause in the sql query to the snippets table in the radiosearch database.
    """
    output_disjuncts = []
    if station_group in CUSTOM_STATION_GROUPS:
        disjuncts = CUSTOM_STATION_GROUPS[station_group]
    else:
        disjuncts = station_group.split(",")
    for disjunct in disjuncts:
        parts = disjunct.split("=")
        if len(parts) == 2:
            lhs, rhs = parts
            if lhs in FACETABLE_COLUMNS:
                output_disjuncts.append("%s='%s'" % (lhs, rhs))
    return output_disjuncts and ["(" + " OR ".join(output_disjuncts) + ")"] or []


def station_to_station_groups(station_metadata):
    """Given a station metadata record, determine all the aggregations it's in.
    For example WUWG -> ["state_code=GA", "callsign="WUWG", "Southeast_U.S."]
    """
    groups = []
    if "state" in station_metadata:
        groups.append("state_code=" + station_metadata["state"])
    if "callsign" in station_metadata:
        groups.append("callsign=" + station_metadata["callsign"])
    for group_key, group_disjuncts in CUSTOM_STATION_GROUPS.items():
        for group in groups:
            if group in group_disjuncts:
                groups.append(group_key)
                break
    return groups


def get_sqs():
    return boto3.resource("sqs", region_name=REGION_NAME)


def get_s3():
    return boto3.resource("s3", region_name=REGION_NAME)


def facet_value_to_display(val):
    """Facet values are strings like "diary_gender=F" or "state_code=MA".  This
    method formats a single facet value (not a compound) for display to humans.
    """
    parts = val.split("=")
    if len(parts) == 2:
        lhs, rhs = parts
        if lhs == "state_code":
            return STATE_PREFIXES.get(rhs, rhs)
        elif lhs == "diary_gender":
            return "All clips with predicted gender " + (
                rhs == "F" and "Female" or "Male"
            )
        elif lhs == "diary_band":
            return "All clips with " + (
                rhs == "T"
                and "telephone speech"
                or (rhs == "S" and "studio speech" or "Unclassified speech")
            )
        else:
            return rhs
    return val.replace("_", " ")


def audio_key_to_station(key):
    # keys are like:  speechbox/stream_out/2018-03-03/WARA/15_50_48.raw
    key_parts = key.split("/")
    if len(key_parts) >= 4:
        return key.split("/")[3]
    return "unknown"


def audio_key_to_datestring(key):
    return key.split("/")[2]


def audio_key_to_timestamp(key):
    """Given a key in the format above, extract a timestamp like "2018-03-03 15:50:48".
    """
    parts = key.split("/")
    date_str = parts[2] + " " + parts[4].split(".")[0].replace("_", ":")
    return date_str


def timestamp_to_epoch(s):
    return float(datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").strftime("%s"))


def fetch_reco_result_for_audio_key(audio_key, s3_resource):
    """Loads the reco result data from S3.  For results prior to late May, 2018,
    only a word-segment TSV file is available at SEGMENTS_SUFFIX.  After that we
    starting writing out structured data to RESULTS_SUFFIX.
    """
    try:
        obj = s3_resource.Object(BUCKET_NAME, audio_key + RESULTS_SUFFIX)
        content = obj.get()["Body"].read().decode("utf-8")
        return json.loads(content)
    except Exception as e:
        # NB: going rogue and catching Exception here because it's been exceptionally hard
        # to find the right exception to catch for this. Lots of nonsense on
        # https://stackoverflow.com/questions/33842944/check-if-a-key-exists-in-a-bucket-in-s3-using-boto3
        # but none of it works correctly.
        obj = s3_resource.Object(BUCKET_NAME, audio_key + SEGMENTS_SUFFIX)
        try:
            content = obj.get()["Body"].read().decode("utf-8")
            return {"words": parse_tsv(content)}
        except Exception as e:
            return None


def fetch_audio_file(audio_key, s3_resource):
    try:
        obj = s3_resource.Object(BUCKET_NAME, audio_key).get()
        return obj
    except s3_resource.meta.client.exceptions.NoSuchKey as e:
        raise


def parse_tsv(content):
    """ Parse content from a word segment TSV file (lines of the form word,start time,end time).
    """
    res = []
    for line in content.split("\n"):
        parts = line.split("\t")
        if len(parts) == 3:
            res.append([parts[0], float(parts[1]), float(parts[2])])
    return res


def reco_results_to_segments(result, audio_key):
    if "diarization" in result and "words" in result:
        # Diarization info is present;  prioritize that.
        yield from result_to_diarization_segments(result, audio_key)
    elif "words" in result:
        # Only word timings are available
        yield from words_to_silence_based_segments(result["words"], audio_key)


def result_to_diarization_segments(result, audio_key, clip_start_time=None):
    """Here we reconcile the diarization segments (in result["diarization"]) with the
    recognizer's word-level segments (in result["words"]).  For each diarization segment (t1, t2),
    output the sequence of words that overlap with (t1, t2), excluding any that have already
    been output; hence, if a diarization segment falls within a word boundary, that word
    is associated with the earlier segment.
    If no clip_start_time is provided, it is inferred from the audio_key.
    """
    word_start_stop_conf_tuples = result["words"]
    word_idx = 0  # index into word segments
    segment_idx = 0  # output index of segment
    if clip_start_time:
        clip_start_global = clip_start_time
    else:
        clip_start_global = timestamp_to_epoch(audio_key_to_timestamp(audio_key))
    result["diarization"].sort(key=lambda x: x.get("start_seconds", 0.0))
    for diary_entry in result["diarization"]:
        start_sec, end_sec = (
            diary_entry.get("start_seconds", 0.0),
            diary_entry.get("end_seconds"),
        )
        # Advance to the word that contains start_sec
        while (
            word_idx < len(word_start_stop_conf_tuples)
            and start_sec > word_start_stop_conf_tuples[word_idx][1]
        ):
            word_idx += 1
        segment = []
        mean_word_confidence = 0.0
        while (
            word_idx < len(word_start_stop_conf_tuples)
            and end_sec > word_start_stop_conf_tuples[word_idx][1]
        ):
            segment.append(word_start_stop_conf_tuples[word_idx][0])
            if len(word_start_stop_conf_tuples[word_idx]) > 3:
                mean_word_confidence += word_start_stop_conf_tuples[word_idx][3]
            word_idx += 1
        if len(segment) > 0:
            mean_word_confidence /= len(segment)
        else:
            mean_word_confidence = -1.0
        segstr = " ".join(segment)
        yield (
            {
                "audio_key": audio_key,
                "segment_idx": segment_idx,
                "content": segstr,
                "segment_start_global": clip_start_global + start_sec,
                "segment_end_global": clip_start_global + end_sec,
                "segment_start_relative": start_sec,
                "mean_word_confidence": mean_word_confidence,
                "diary_speaker_id": diary_entry.get("spkr_label", ""),
                "diary_gender": diary_entry.get("gender", ""),
                "diary_env": diary_entry.get("env_type", ""),
                "diary_band": diary_entry.get("band_type", ""),
            }
        )
        segment_idx += 1


def words_to_silence_based_segments(word_start_stop_conf_tuples, audio_key):
    """Segments the transcript into sentence-like segments based purely
    on silence.  When real diarization info is available, that should be prefered
    to this method.
    """
    from cortico.components.speech.common import transcript_util

    word_start_stop_conf_tuples.append(("END_SEGMENT", 0.0, 0, 0))
    segment = []
    last_stop = 0.0
    segment_start = 0.0
    clip_start_global = timestamp_to_epoch(audio_key_to_timestamp(audio_key))

    idx = 0
    for row in word_start_stop_conf_tuples:
        if len(row) < 3:
            continue
        w, start, stop = row[0], float(row[1]), float(row[2])
        if transcript_util.is_silence_candidate(w):
            continue
        if (start - last_stop > 0.5 and len(segment) >= 5) or w == "END_SEGMENT":

            # If add_sentence_markers is True below, we try to do some lightweight NLP-based
            # sentence segmentation on each silence-delimited chunk of reco transcript text.
            # It doesn't work that well so it's False for now, meaning that only silence
            # (start-last_stop above) and MAX_OUTPUT_SENTENCE_LENGTH are used to determine
            # segment boundaries.
            processed = transcript_util.postprocess_reference(
                " ".join(segment), add_sentence_markers=False
            )

            # Add sentence delimiters around segment
            processed.insert(0, "<s>")
            processed.append("</s>")

            subseg = ""
            for w2 in processed:
                if w2 == "</s>":
                    yield (
                        {
                            "audio_key": audio_key,
                            "segment_idx": idx,
                            "content": subseg.strip(),
                            "segment_start_global": clip_start_global + segment_start,
                            "segment_start_relative": segment_start,
                        }
                    )
                    idx += 1
                    subseg = ""
                elif w2 != "<s>":
                    subseg += w2 + " "
                if len(subseg) >= MAX_OUTPUT_SENTENCE_LENGTH:
                    yield (
                        {
                            "audio_key": audio_key,
                            "segment_idx": idx,
                            "content": subseg.strip(),
                            "segment_start_global": clip_start_global + segment_start,
                            "segment_start_relative": segment_start,
                        }
                    )
                    idx += 1
                    subseg = ""
            segment = []
            segment_start = start
        segment.append(w)
        last_stop = stop


def get_search_match_window(content, query, window_size=2):
    """Get a window of words around the portion of content that matches the query.
    window_size is exclusive of the matched words
    This closely mirrors the snippet highlighting code in the frontend:
    frontend/src/components/RadioSearchResults/RadioSearchResults.js (renderSnippet)
    """
    if not query:
        return content
    content = re.sub(r"[^0-9A-Za-z\'_\.@% ]", "", content).lower().split()
    query = re.sub(r"[^0-9A-Za-z\'_\.@% ]", "", query).lower()
    split_index = 0
    maxj = 0
    for i in range(len(content)):
        suffix = " ".join(content[i:])
        j = 0
        while j < len(query) and j < len(suffix) and query[j] == suffix[j]:
            j += 1
        if j > maxj:
            maxj = j
            split_index = i
    match_window = content[
        max(0, split_index - window_size) : min(
            split_index + window_size + 1, len(content)
        )
    ]
    return " ".join(match_window)


def score_and_rerank_snippets(result_list):
    """Implements the scoring function used to determine how radiosearch snippet results
    are ordered.  Adds the score to each result and then resorts the list.
    """
    for res in result_list:
        is_telephone = res.get("diary_band", "") == "T"
        is_nonsyndicated = res.get("synd_class", "") == "unique"
        start_time_epoch = timestamp_to_epoch(res.get("segment_start_time", 0))
        score = (is_telephone * 2 + is_nonsyndicated) * 1e8 + (start_time_epoch / 1e6)
        res["priority_score"] = score
    result_list.sort(key=lambda x: x["priority_score"], reverse=True)
    return result_list
