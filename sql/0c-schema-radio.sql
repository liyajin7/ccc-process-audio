/*
 * Final station, show, snippet data tables
 */

-- pk and fk constraints on many of these tables are added later
-- in the load process to speed things up, but listed here
-- for completeness

drop table if exists radio.show cascade;
create table radio.show
(
    show_id bigint
        generated by default as identity
        primary key,
    parent_show_id bigint -- nullable
        references radio.show
        deferrable initially immediate,

    name varchar(256) not null,

    is_music boolean not null default false,
    is_sports boolean not null default false,
    exclude boolean not null default false,

    insert_dt timestamptz not null default now()
);
create unique index on radio.show ( trim(lower(name)) );

drop table if exists radio.station cascade;
create table radio.station
(
    station_id bigint
        generated by default as identity
        primary key,

    state_id int not null
        references dim.state
        deferrable initially immediate,

    callsign char(4) not null,
    band char(2) not null check(band in ('AM', 'FM', 'FL')),
    freq numeric not null,
    is_licensed boolean not null default true,

    format text,
    raw_owner text,
    std_owner text,

    ingested boolean not null default false,
    exclude boolean default false check(
        -- i.e., exclude is null iff ingested is null
        (not (exclude is not null and not ingested)) and
        (not (exclude is null and ingested))
    ),

    insert_dt timestamptz not null default now(),

    unique(callsign, band) deferrable initially immediate
);

drop table if exists radio.station_map cascade;
create table radio.station_map
(
    station_map_id bigint
        generated by default as identity
        primary key,

    station_id int not null
        references radio.station
        deferrable initially immediate,
    time_of_day char(1) not null,
    strength varchar(16) not null,

    transmitter_latitude numeric not null,
    transmitter_longitude numeric not null,
    wkt text not null,

    insert_dt timestamptz not null default now(),

    check(time_of_day in ('A', 'D', 'N', 'C')),
    check(strength in ('weak', 'moderate', 'strong')),

    unique (station_id, time_of_day, strength) deferrable initially immediate
);

drop table if exists radio.snippet cascade;
create table radio.snippet
(
    snippet_id bigint
        generated by default as identity
        primary key,

    --
    -- FKs to other tables
    --

    station_id bigint not null,
        -- references radio.station
        -- deferrable initially immediate,
    show_id bigint, -- nullable
        -- references radio.show
        -- deferrable initially immediate,

    --
    -- fields from snippet files
    --

    audio_file_offset numeric not null,
    audio_file_index int not null,

    content text not null,
    approxhash varchar(256),

    audio_key varchar(256),
    raw_callsign varchar(16),
    raw_show_name varchar(128),
    show_confidence numeric,
    diary_speaker_id varchar(16),
    diary_band varchar(16),
    diary_env varchar(16),
    mean_word_confidence numeric,
    diary_gender varchar(16),
    show_source text,

    --
    -- fields precomputed for convenience
    --

    date date not null,
    wordcount int not null check(wordcount >= 0),
    duration numeric not null check(duration >= 0),
    start_dt timestamptz not null,
    end_dt timestamptz not null,

    insert_dt timestamptz not null default now()
);

--
-- Generated data about syndication
--

drop table if exists radio.snippet_syndication cascade;
create table radio.snippet_syndication
(
    syndication_method varchar(32) not null,
    snippet_id bigint not null,
        -- references radio.snippet
        -- deferrable initially immediate,

    syndicated boolean not null,

    insert_dt timestamptz not null default now()

    -- primary key (syndication_method, snippet_id)
    -- deferrable initially immediate
);

