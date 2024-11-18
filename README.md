# event-lifecycles
Event lifecycles paper

This repository contains analysis and data preparation code for the paper "The
speed of news in Twitter versus radio".

A short overview of the code:
* The 2019 and 2020 dataset, for both radio and Twitter, was initially prepared
  first for an earlier project. This dataset was put together from raw data
  files according to the steps in `setup.sh`, relying on some SQL in the `sql/`
  directory.
* We subsequently prepared 2021 and 2022 data for both media using scripts and
  notebooks in the `notebooks/data-prep` directory.
* It quickly became clear that we had too little radio data collected
  during 2022 to use, and we prepared the paper using only 2019-2021 data. Radio
  collection was sharply ramped down in late 2021 for budget reasons.
* Notebooks in the `notebooks/` directory were used for analysis.
    * The 1\* notebooks carry out analysis of manually identified events.
    * The 5\* and 6\* notebooks and scripts run the newsLens algorithm.
    * The 7a\*, 7b\* and 7c\* notebooks calculate story-level statistics
      including empirical CDFs.
    * Further notebooks carry out quality checks of the stories and data, or
      perform analysis reported in the paper.

This work © 2023 by Massachusetts Institute of Technology is licensed under CC
BY 4.0. To view a copy of this license, visit
http://creativecommons.org/licenses/by/4.0/
