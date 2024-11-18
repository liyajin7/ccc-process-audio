/*
 * We want to get solidly attributed episodes of shows which were recognized
 * well - a set of shows usable as a check on analysis using all show_id
 * information
 */

drop table if exists datasets.radio_episode;
create table datasets.radio_episode as
select
    sn.date,
    sn.show_id,
    sn.station_id,

    count(*) as cnt,
    sum(sn.duration) as duration,
    avg(sn.show_confidence) as show_confidence,
    avg(sn.mean_word_confidence) as word_confidence
from radio.snippet_data sn
where
    sn.show_id is not null
group by 1,2,3;

drop table if exists datasets.radio_best_episode;
create table datasets.radio_best_episode as
select
    x.date,
    x.show_id,
    x.station_id,

    x.cnt,
    x.duration,
    x.show_confidence,
    x.word_confidence
from
(
    select
        te.date,
        te.show_id,
        te.station_id,

        te.cnt,
        te.duration,
        te.show_confidence,
        te.word_confidence,

        row_number() over (
            partition by te.date, te.show_id
            order by
                te.show_confidence desc,
                te.cnt desc,
                te.word_confidence desc
        ) as quality
    from datasets.radio_episode te
    where
        te.word_confidence >= 0.85 and

        te.show_confidence >= 0.7 and

        te.cnt >= 300 and
        te.cnt <= 2000 and

        te.duration >= 3200 and
        te.duration <= 18000
) x
where
    x.quality = 1;

drop table if exists datasets.radio_best_episode_confidence_only;
create table datasets.radio_best_episode_confidence_only as
select
    x.date,
    x.show_id,
    x.station_id,

    x.cnt,
    x.duration,
    x.show_confidence,
    x.word_confidence
from
(
    select
        te.date,
        te.show_id,
        te.station_id,

        te.cnt,
        te.duration,
        te.show_confidence,
        te.word_confidence,

        row_number() over (
            partition by te.date, te.show_id
            order by
                te.show_confidence desc,
                te.cnt desc,
                te.word_confidence desc
        ) as quality
    from datasets.radio_episode te
    where
        te.word_confidence >= 0.85 and
        te.show_confidence >= 0.7
) x
where
    x.quality = 1;

drop table if exists datasets.radio_best_episode_all;
create table datasets.radio_best_episode_all as
select
    x.date,
    x.show_id,
    x.station_id,

    x.cnt,
    x.duration,
    x.show_confidence,
    x.word_confidence
from
(
    select
        te.date,
        te.show_id,
        te.station_id,

        te.cnt,
        te.duration,
        te.show_confidence,
        te.word_confidence,

        row_number() over (
            partition by te.date, te.show_id
            order by
                te.show_confidence desc,
                te.cnt desc,
                te.word_confidence desc
        ) as quality
    from datasets.radio_episode te
) x
where
    x.quality = 1;

