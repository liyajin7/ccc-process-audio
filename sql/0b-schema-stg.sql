/*
 * Staging tables for data about stations and snippets
 */

drop table if exists stg.station;
create table stg.station
(
    station_id bigint not null unique,
    state char(2) not null,
    callsign char(4) not null,
    band char(2) not null,
    freq numeric not null,

    stream_url text,
    city text,
    school text,
    format text,
    website text,
    owner text,
    address text,
    phone text,
    fax text,
    status text,
    power text,
    height text,
    antenna_pattern text,
    license_granted text,
    license_expires text,
    last_fcc_update text,
    is_licensed boolean not null default true
);

drop table if exists stg.station_map;
create table stg.station_map
(
    station_map_id bigint not null unique,
    station_id int not null,
    time_of_day char(1) not null,
    latitude numeric not null,
    longitude numeric not null
);

drop table if exists stg.station_map_coordinate;
create table stg.station_map_coordinate
(
    station_map_coordinate_id bigint not null unique,
    station_map_id bigint not null,
    strength varchar(16) not null,
    latitude numeric not null,
    longitude numeric not null
);

drop table if exists stg.station_ownership;
create table stg.station_ownership
(
    station_id bigint not null unique,
    in_map boolean not null check(in_map), -- DB-enforced sanity check

    callsign varchar(16),
    band varchar(16),
    freq varchar(16),
    is_public boolean,
    original_owner varchar(256),
    std_owner varchar(256) not null,
    exclude boolean not null,
    notes text
);

drop table if exists stg.show_twitter;
create table stg.show_twitter
(
    show_id bigint not null unique,
    name text not null unique,
    snippet_count text,
    parent_show_id bigint,
    is_music boolean not null,
    is_sports boolean not null,
    exclude boolean not null,
    twitter text,
    host text,
    show text,
    notes text
);

drop table if exists stg.callsign_map;
create table stg.callsign_map
(
    corpus_callsign varchar(8) not null unique,
    ambiguous boolean,
    station_id bigint not null unique
);

drop table if exists stg.snippet;
create unlogged table stg.snippet
(
    callsign varchar(8) not null,
    segment_start_global numeric not null,
    segment_end_global numeric not null,

    content text,
    approxhash varchar(256),

    show_name varchar(256),
    audio_key varchar(256),

    segment_start_relative numeric,
    segment_idx int,

    show_confidence numeric,
    diary_speaker_id text,
    diary_band text,
    diary_env text,
    mean_word_confidence numeric,
    diary_gender text,
    show_source text
);

drop table if exists stg.election_results;
create unlogged table stg.election_results
(
    precinct_id int not null unique,

    votes_pres_dem int not null,
    votes_pres_rep int not null,
    votes_pres_total int not null,

    state_fips char(2) not null,
    wkt text not null
);

drop table if exists stg.census_variable;
create unlogged table stg.census_variable
(
    variable text not null,
    description text
);

drop table if exists stg.census_geo;
create unlogged table stg.census_geo
(
    geoid text not null,
    name text not null,
    state_fips text,
    county_fips text,
    tract_code text,
    block_group_code text,
    land_area text,
    water_area text,
    wkt text not null
);

drop table if exists stg.census_data;
create unlogged table stg.census_data
(
    geoid text not null,
    variable text not null,
    value text
);

