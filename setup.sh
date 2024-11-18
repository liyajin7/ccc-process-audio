#!/bin/bash

set -xe

SCRIPTPATH=$(dirname "$(readlink -f "$0")")
export PATH="$(readlink -f "$SCRIPTPATH"/bin):$PATH"

##
## Radio data sanity checking and phrase detection
##

jsoncheck data/raw/radio/

phrase train -l -c 50 -t 0.75 -o english \
             -m data/models/phrases.model \
             -g npmi data/raw/radio/

phrase export -l -d '_' -s data/models/phrases.model \
              data/metadata/phrases/detected.csv

##
## Create DB schema
##

psql -X -v ON_ERROR_STOP=1 -f sql/0a-schema-setup.sql
psql -X -v ON_ERROR_STOP=1 -f sql/0b-schema-stg.sql
psql -X -v ON_ERROR_STOP=1 -f sql/0c-schema-radio.sql
psql -X -v ON_ERROR_STOP=1 -f sql/0d-schema-twitter.sql
psql -X -v ON_ERROR_STOP=1 -f sql/0e-schema-election.sql
psql -X -v ON_ERROR_STOP=1 -f sql/0f-schema-census.sql

##
## Load election data
##

for f in data/raw/election_results_2016/*; do
    pv "$f" | zcat | csv2pg -s ',' stg.election_results
done

psql -X -v ON_ERROR_STOP=1 -f sql/1-transform-election-data.sql

##
## Load census data
##

for f in data/raw/acs2016/variable/*; do
    pv "$f" | zcat | csv2pg -s ',' stg.census_variable
done

for f in data/raw/acs2016/data/*; do
    pv "$f" | zcat | csv2pg -s ',' stg.census_data
done

for f in data/raw/acs2016/geo/*; do
    pv "$f" | zcat | csv2pg -s ',' stg.census_geo
done

psql -c 'create table stg.tmp_census_variables (name text);'
csv2pg -s ',' stg.tmp_census_variables data/metadata/selected-census-variables.csv
psql -X -v ON_ERROR_STOP=1 -f sql/2-transform-census-data.sql
psql -c 'drop table stg.tmp_census_variables;'

##
## Load radio data
##

## Dim data

csv2pg -s '	' stg.station data/metadata/radio/main.csv
csv2pg -s '	' stg.station data/metadata/radio/extra.csv

csv2pg -s '	' stg.station_map data/metadata/radio/maps.csv
csv2pg -s '	' stg.station_map_coordinate data/metadata/radio/map_coordinates.csv

csv2pg -s ',' stg.callsign_map data/metadata/radio/callsign_map.csv
csv2pg -s ',' stg.station_ownership data/metadata/radio/ownership.csv
csv2pg -s ',' stg.show_twitter data/metadata/radio/show_twitter.csv

psql -X -v ON_ERROR_STOP=1 -f sql/3-transform-stations-shows.sql

## Snippet data

pv data/raw/radio/* | zcat \
                    | jq -c -r -M 'del(.denorm_content,.city,.state)' \
                    | phrase combine -d '_' -f json -n 'content' \
                                -p data/metadata/phrases/enwiki-entities.csv \
                                -p data/metadata/phrases/detected.csv \
                    | approxhash -f json -c content -k approxhash \
                    | json2csv \
                    | csv2pg -s ',' stg.snippet

psql -X -v ON_ERROR_STOP=1 -f sql/4-transform-snippets.sql

## Postprocessing

psql -X -v ON_ERROR_STOP=1 -f sql/5-syndication.sql
psql -X -v ON_ERROR_STOP=1 -f sql/6-finalize.sql
psql -X -v ON_ERROR_STOP=1 -f sql/7-best-episodes.sql
psql -X -v ON_ERROR_STOP=1 -f sql/8a-show-station-fractions.sql
psql -X -v ON_ERROR_STOP=1 -f sql/8b-election-radio-crosswalk.sql
psql -X -v ON_ERROR_STOP=1 -f sql/8c-census-radio-crosswalk.sql
psql -X -v ON_ERROR_STOP=1 -f sql/8d-election-radio-data.sql
psql -X -v ON_ERROR_STOP=1 -f sql/8e-census-radio-data.sql
psql -X -v ON_ERROR_STOP=1 -f sql/9-top-word-counts-radio.sql

##
## Load twitter data
##

# NOTE: commented out because dangerous! drops all data!
# twitter initialize -y

psql -c "select unnest(string_to_array(replace(twitter, ' ', ''), ',')) from stg.show_twitter;" \
     -X -v ON_ERROR_STOP=1 -t --csv | xargs twitter -v user_info -u radio -n

psql -X -v ON_ERROR_STOP=1 -f sql/10-show-twitter.sql

# Look up other users - hand-selected
for f in data/metadata/twitter/users/*; do
    tag="$(basename "$f" .csv)"
    tail -n +2 "$f" | xargs twitter -v user_info -u "$tag" -n
done

# Look up other users - various lists of political figures and journalists
tail -n +2 < data/metadata/twitter/lists.csv | while IFS= read -r list; do
    twitter -v user_info -l "$list" -u "$list"
done

## Tag these users for ease of reference
psql << EOF
begin;

insert into twitter.user_tag
    (user_id, tag)
select
    u.user_id,
    'universe'
from twitter.user;

commit;
EOF

## Users' tweets
twitter -v tweets -g universe

## Users' friends
twitter -v friends -g universe

## Users' followers
twitter -v followers -g universe

##
## Samples
##

mkdir -p data/samples/twitter/
mkdir -p data/samples/radio/

# NOTE also pull the decahose samples by hand via Athena or hive

## Radio
pg2csv -f sql/samples/radio/station-pairs.sql -o data/samples/radio/station-pairs.csv
pg2csv -f sql/samples/radio/show-pairs.sql -o data/samples/radio/show-pairs.csv
pg2csv -f sql/samples/radio/show-pairs-content.sql -o data/samples/radio/show-pairs-content.csv
pg2csv -f sql/samples/radio/all-local-content.sql -o data/samples/radio/all-local-content.csv
pg2csv -f sql/samples/radio/all-syndicated-content.sql -o data/samples/radio/all-syndicated-content.csv
pg2csv -f sql/samples/radio/cooccurrence.sql -o data/samples/radio/cooccurrence.csv
pg2csv -f sql/samples/radio/election-results-show.sql -o data/samples/radio/election-results-show.csv
pg2csv -f sql/samples/radio/election-results-station.sql -o data/samples/radio/election-results-station.csv
pg2csv -f sql/samples/radio/show-data.sql -o data/samples/radio/show-data.csv
pg2csv -f sql/samples/radio/station-data.sql -o data/samples/radio/station-data.csv
pg2csv -f sql/samples/radio/episode-airtime.sql -o data/samples/radio/episode-airtime.csv
pg2csv -f sql/samples/radio/station-maps.sql -o data/samples/radio/station-maps.csv
pg2csv -f sql/samples/radio/census-data-station.sql -o data/samples/radio/census-data-station.csv
pg2csv -f sql/samples/radio/census-data-show.sql -o data/samples/radio/census-data-show.csv
pg2csv -f sql/samples/radio/best-episodes.sql -o data/samples/radio/best-episodes.csv

## Twitter
pg2csv -f sql/samples/twitter/user-data.sql -o data/samples/twitter/user-data.csv
pg2csv -f sql/samples/twitter/tweets.sql -o data/samples/twitter/tweets.csv
pg2csv -f sql/samples/twitter/user-show.sql -o data/samples/twitter/user-show.csv
pg2csv -f sql/samples/twitter/tweet-activity-by-day.sql -o data/samples/twitter/tweet-activity-by-day.csv
pg2csv -f sql/samples/twitter/follow-graph.sql -o data/samples/twitter/follow-graph.csv
pg2csv -f sql/samples/twitter/follow-graph-multiple-radio.sql -o data/samples/twitter/follow-graph-multiple-radio.csv
pg2csv -f sql/samples/twitter/mention-graph.sql -o data/samples/twitter/mention-graph.csv
pg2csv -f sql/samples/twitter/reply-graph.sql -o data/samples/twitter/reply-graph.csv
pg2csv -f sql/samples/twitter/retweet-graph.sql -o data/samples/twitter/retweet-graph.csv
pg2csv -f sql/samples/twitter/all-mentions.sql -o data/samples/twitter/all-mentions.csv
pg2csv -f sql/samples/twitter/all-replies.sql -o data/samples/twitter/all-replies.csv
pg2csv -f sql/samples/twitter/all-retweets.sql -o data/samples/twitter/all-retweets.csv
pg2csv -f sql/samples/twitter/mutual-friends.sql -o data/samples/twitter/mutual-friends.csv
pg2csv -f sql/samples/twitter/mutual-followers.sql -o data/samples/twitter/mutual-followers.csv
pg2csv -f sql/samples/twitter/friends-of-radio-followers.sql -o data/samples/twitter/friends-of-radio-followers.csv

