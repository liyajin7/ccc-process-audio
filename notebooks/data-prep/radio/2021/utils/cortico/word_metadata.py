#!/usr/bin/env python3

"""
Manages various maps associated with individual words and multi-word tokens (collectively
called "terms") used by the radiosearch backend.

To regenerate unigram_counts.tsv, run search/pull_radio_reco_results.py and then:
   cat snippets_with_synd.tsv | \
   awk '{FS="\t"; cf=split($2,ar," "); for(i=1;i<=cf;i++) print ar[i];}' | \
   sort | uniq -c | sort -nr | awk '{print $2 "\t" $1;}' > unigram_counts.tsv

To renegerate word_denorm.tsv, run lm/pull_transcripts.py
For more info on entities, see speech/data/entities/readme.md
For more info on tags, see speech/search/modeling/generate_tags.py
"""

import csv
import os

from yaml import safe_load as yaml_load

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data"))
CONFIG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../config"))
COUNT_FILE = os.path.join(DATA_DIR, "unigram_counts.tsv")
DENORM_FILE = os.path.join(DATA_DIR, "word_denorm.tsv")
ENTITY_FILE_DIR = os.path.join(DATA_DIR, "entities")
TAG_DATA_FILE = os.path.join(DATA_DIR, "tags.tsv")
TAG_CONFIG_FILE = os.path.join(CONFIG_DIR, "tags.yml")

# What the recognizer hypothesizes for unknown words
UNKNOWN_TOKEN = "<unk>"

STOPWORD_MIN_PROB = 0.001


class WordMetadata:
    def __init__(self, logger=None):
        self.word_denorm_map = {}
        self.stopwords_set = set()
        self.words_that_start_ngrams = set()
        self.tag_id_to_terms = {}
        self.term_to_tags = {}
        self._tag_to_display_map = {}
        self._read_unigram_counts()
        self._read_denorm_data()
        self._read_entities()
        self._read_tags()

    def _read_unigram_counts(self):
        """Read a map of word -> count.  As of now, this file is only used for determining
        stopwords."""
        word_unigram_counts = {}
        with open(COUNT_FILE) as tsvin:
            tsvin = csv.reader(tsvin, delimiter="\t")
            count_sum = 0
            for row in tsvin:
                count = int(row[1])
                word_unigram_counts[row[0]] = count
                count_sum += count
            for word, count in word_unigram_counts.items():
                if (count / count_sum) >= STOPWORD_MIN_PROB:
                    self.stopwords_set.add(word)

    def _read_denorm_data(self):
        """Read a map of term -> denorm, e.g. mcsweeney -> McSweeney."""
        with open(DENORM_FILE) as tsvin:
            tsvin = csv.reader(tsvin, delimiter="\t")
            for row in tsvin:
                norm_form, denorm_form = row[0:2]
                if norm_form.count(" ") != denorm_form.count(" "):
                    if logger:
                        logger.warn(
                            "In %s, the word '%s' denorms to '%s', "
                            + "which has a different number of tokens. Skipping.",
                            DENORM_FILE,
                            norm_form,
                            denorm_form,
                        )
                    continue
                self.word_denorm_map[norm_form] = denorm_form
                if norm_form.find(" ") != -1:
                    self.words_that_start_ngrams.add(norm_form.split(" ")[0])
        self.word_denorm_map["<unk>"] = "..."

    def _read_entities(self):
        """Add "entities", words and phrases treated specially by the recognizer."""
        for entity in self.get_entities():
            norm_form = self.norm(entity)
            if norm_form not in self.word_denorm_map:
                first_word = norm_form.split(" ")[0]
                if (
                    first_word not in self.word_denorm_map
                    and first_word.find("._") == -1
                ):
                    # This is a total hack to counteract the problem that our compounds are
                    # from Wikipedia and Wikipedia title cases everything, even when the
                    # first word is a common noun, like "Gun control".  We want to recognize
                    # these as compounds but back off to the denorms for the individual words
                    # in the compounds
                    entity = " ".join(
                        [self.word_denorm_map.get(x, x) for x in norm_form.split(" ")]
                    )

                self.word_denorm_map[norm_form] = entity
                self.word_denorm_map[entity.lower()] = entity
                if norm_form.find(" ") != -1:
                    self.words_that_start_ngrams.add(first_word)
                    self.words_that_start_ngrams.add(entity.lower().split()[0])

    def _read_tags(self):
        """Read tag groups"""
        with open(TAG_CONFIG_FILE, "r") as finp:
            tag_config = yaml_load(finp)
        for tag_info in tag_config["tags"]:
            self._tag_to_display_map[tag_info["id"]] = tag_info["display_name"]
        with open(TAG_DATA_FILE) as tsvin:
            tsvin = csv.reader(tsvin, delimiter="\t")
            for row in tsvin:
                tag_id, term = row[0:2]
                if tag_id not in self.tag_id_to_terms:
                    self.tag_id_to_terms[tag_id] = []
                if term not in self.term_to_tags:
                    self.term_to_tags[term] = []
                self.tag_id_to_terms[tag_id].append(term)
                self.term_to_tags[term].append(tag_id)

    def tag_to_display_name(self, tag):
        return self._tag_to_display_map.get(tag, tag)

    def denorm(self, word, next_word=None):
        """
        Denormalizes an individual word, or a word pair.
        Note: If next_word is set, we'll check if "<word> <next_word>" is a bigram
        in the denormalization table and return its bigram denorm.  There will be
        a space in the result if and only if this occurs.
        """
        if word in self.word_denorm_map:
            x = self.word_denorm_map[word]
        elif word.find("._") != -1:
            # acronym like n._p._r.-> NPR.   n._p._r.'s -> NPR's
            parts = word.split(".")
            x = "".join([x.replace("_", "").upper() for x in parts[:-1]] + [parts[-1]])
        elif word.find("_") != -1 and word.replace("_", " ") in self.word_denorm_map:
            x = self.word_denorm_map[word.replace("_", " ")]
        elif word.endswith("."):
            x = word[0].capitalize() + word[1:]
        else:
            x = word
        if (
            next_word is not None
            and word in self.words_that_start_ngrams
            and (word + " " + next_word) in self.word_denorm_map
        ):
            x = self.word_denorm_map[word + " " + next_word]

        # If there's still a "_" this may be a compound; return individual denorms
        # separated by space
        if x.find("_") != -1 and not next_word:
            x = " ".join([self.denorm(y) for y in word.split("_")])

        return x

    def denorm_text(self, content, always_capitalize_first=True):
        """Denormalize an arbitrary text string."""
        res = " ".join(self.get_denorm_terms(content))
        if always_capitalize_first and res:
            res = res[0].capitalize() + res[1:]
        return res

    def get_denorm_terms(self, content):
        """Returns the sequence of single and multi-word compounds for content."""
        content_words = content.split()
        denorm_content_words = []
        skip_next_word = False
        for i, word in enumerate(content_words):
            if skip_next_word:
                skip_next_word = False
                continue
            if i < len(content_words) - 1:
                next_word = content_words[i + 1]
            else:
                next_word = None
            denorm_word = self.denorm(word, next_word=next_word)
            skip_next_word = denorm_word.find(" ") != -1
            denorm_content_words.append(denorm_word)
        return denorm_content_words

    @staticmethod
    def get_entities(logger=None, exclude_substr=None):
        entities = set()
        # Add "entities", words and phrases treated specially by the recognizer
        for filename_base in os.listdir(ENTITY_FILE_DIR):
            if logger:
                logger.info("Loading entity file: %s", filename)
            if not filename_base.endswith(".entities") or (
                exclude_substr and filename_base.find(exclude_substr) != -1
            ):
                continue
            filename = os.path.join(ENTITY_FILE_DIR, filename_base)
            with open(filename) as tsvin:
                tsvin = csv.reader(tsvin, delimiter="\t")
                for row in tsvin:
                    entities.add(row[0])
        # Also add entities from tags config file
        with open(TAG_CONFIG_FILE, "r") as finp:
            tag_config = yaml_load(finp)
        for tag_info in tag_config["tags"]:
            if "values" in tag_info:
                for val in tag_info["values"]:
                    entities.add(val)
        return list(entities)

    @staticmethod
    def norm(word):
        """Map any display string to the form used in reco transcripts, which is
        the form we use for indexing all data structures in word_metadata"""
        newtoks = []
        toks = word.split(" ")
        for tok in toks:
            if len(tok) >= 2 and tok.upper() == tok:
                # Acronym.  NPR -> n._p._r.
                x = "._".join(tok.lower()) + "."
            else:
                x = tok.lower().replace(",", "")
            newtoks.append(x)
        return " ".join(newtoks)

    def is_content_word(self, w):
        return (
            w != UNKNOWN_TOKEN
            and w not in self.stopwords_set
            and w.find("[") == -1
            and len(w) > 2
        )
