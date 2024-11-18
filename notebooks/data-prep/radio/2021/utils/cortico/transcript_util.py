#!/usr/bin/env python3

"""
Common methods for normalizing the text in reference transcripts
(i.e., human-transcribed speech transcripts) and recognizer output.
Note: The reference-transcript processing is fairly specialized to
Talk-of-the-Nation transcripts for now.
"""

import hashlib
import re

# Remove these words from both the ref and hyp transcripts before aligning.
# These are mostly disfluencies on the hyp side.
DISFLUENCY_WORDS = ["uh", "um", "ah", "er", "mm"]

# Words which are considered equivalent to silence for the purpose of
# sentence segmentation
SILENCE_CANDIDATE_WORDS = ["noise"]

SKETCH_MAX_INPUT_CHARS = 32
SKETCH_OUTPUT_CHARS = 8

global_norm_counts = {}  # norm_word -> display_word -> count

# Internal token meaning "display this word the same as its normalized value"
DO_NOT_NORMALIZE_TOKEN = "_"


def tokenize(str):
    str = re.sub(r"[^0-9A-Za-z\'_\.@%<>]", " ", str).lower()
    return [w.strip(".") for w in str.split()]


def tokenize_preserve_case(str):
    str = re.sub(r"[^0-9A-Za-z\'_\.@%<>]", " ", str)
    return [w.strip(".") for w in str.split()]


def normalize_word(w):
    # "_" is in acronyms like u._s. in the hyp.
    return w.replace("_", "").replace(".", "")


def is_silence_candidate(w):
    """Do we consider the duration that this token was spoken to be like silence
    for the purpose of segmentation?
    """
    return w in SILENCE_CANDIDATE_WORDS


# Hacky.  Should be moved into word_metadata
def allow_word(w):
    return w not in DISFLUENCY_WORDS and w.find("[") == -1


def snippet_signature(t, stopwords_set=None):
    """Map a snippet to a compact signature based on the first few content words.
    This is intended for syndication classification and de-duping.
    """
    if not stopwords_set:
        s = t
    else:
        content_words = []
        for word in t.split():
            if word not in stopwords_set:
                content_words.append(word[:4])
        s = " ".join(content_words)
    if len(s) == 0:
        # Reserved value that means "not enough content"
        return "0"
    s = s[:SKETCH_MAX_INPUT_CHARS]
    return hashlib.sha256(s.encode()).hexdigest()[:SKETCH_OUTPUT_CHARS]


def postprocess_reference(
    input_str,
    add_sentence_markers=False,
    preserve_input_case=False,
    measure_norm_counts=False,
):
    from gensim.summarization import textcleaner

    """This takes a "reference" (human-transcribed) document from the NPR data set and
    cleans it for use by our language modeling pipeline.  If add_sentence_markers is True,
    we split the text into sentences, one-per-line, using gensim.
    It's assumed that a candidate sentence never cross between lines, which is true of the
    NPR data set.  If measure_norm_counts is set, we accumulate global stats on how individual
    tokens and bigrams are most commonly rendered. (This flag is for batch analysis jobs
    and should not be set in production since it consumes a fair bit of memory.)
    """
    out = []
    for line in input_str.split("\n"):
        # Note that we assume copyrights like this only appear in the footer
        if line.startswith("Copyright ©"):
            break
        line = re.sub(r"^.*?[:]", "", line)
        line = re.sub(r"\[.*?\]", "", line)
        line = re.sub(r"\(.*?\)", "", line)
        if measure_norm_counts:
            toks = tokenize_preserve_case(line)
            prev_norm = None
            prev_was_norm = True
            for i, t in enumerate(toks):
                norm_tok = normalize_word(t.lower())
                counted_bigram = False
                a = []

                # Add a bigram count if it's in the bigram set already.
                # Note:  This is an efficiency hack and may slightly overestimate the probability
                # tbat a bigram should be capitalized, since we only accrue count on its norm form
                # from the first time the capitalized form is seen.  But this works fine on large
                # corpuses.
                if i > 0 and "%s %s" % (prev_norm, norm_tok) in global_norm_counts:
                    if prev_was_norm and norm_tok == t:
                        a.append(
                            (DO_NOT_NORMALIZE_TOKEN, "%s %s" % (prev_norm, norm_tok))
                        )
                    else:
                        a.append(
                            (
                                "%s %s" % (toks[i - 1], t),
                                "%s %s" % (prev_norm, norm_tok),
                            )
                        )
                    counted_bigram = True

                # Append pair for isolated word
                if norm_tok == t:
                    a.append((DO_NOT_NORMALIZE_TOKEN, norm_tok))
                    prev_was_norm = True
                else:
                    a.append((t, norm_tok))
                    # Add bigram count if two nonnorm toks in a row and it's not already counted
                    if not prev_was_norm and not counted_bigram:
                        a.append(
                            (
                                "%s %s" % (toks[i - 1], t),
                                "%s %s" % (prev_norm, norm_tok),
                            )
                        )
                    prev_was_norm = False

                prev_norm = norm_tok

                for x_orig, x_norm in a:
                    current_counts = global_norm_counts.get(x_norm, {})
                    current_counts[x_orig] = current_counts.get(x_orig, 0) + 1
                    global_norm_counts[x_norm] = current_counts
        if add_sentence_markers:
            sentences = textcleaner.clean_text_by_sentences(line)
            for s in sentences:
                out += ["\n"] + tokenize(s.text) + [""]
        else:
            if preserve_input_case:
                out += tokenize_preserve_case(line)
            else:
                out += tokenize(line)

    return [normalize_word(w) for w in out if allow_word(w)]


def postprocess_hyp(s):
    out = []
    for word in s.lower().split():
        if allow_word(word):
            out.append(normalize_word(word))
    return out
