drop table if exists tmp_episodes;
create local temporary table tmp_episodes
on commit preserve rows as
select
    rbe.date,
    rbe.show_id,
    rbe.station_id,

    exists
    (
        select
            1
        from twitter.user_show us
        where
            us.show_id = rbe.show_id
    ) as twitter,

    sh.name as show_name,
    (st.format = 'Public Radio') as is_public,

    rbe.cnt,
    rbe.duration,
    rbe.show_confidence,
    rbe.word_confidence
from datasets.radio_best_episode rbe
    inner join radio.station st using(station_id)
    inner join radio.show sh using(show_id);

drop table if exists tmp_samples;
create local temporary table tmp_samples
on commit preserve rows as
select
    te1.date as date1,
    te1.show_id as show_id1,

    te2.date as date2,
    te2.show_id as show_id2
from tmp_episodes te1
    inner join tmp_episodes te2 using(date)
where
    te1.show_id > te2.show_id

union all

-- this is a sanity check: are episodes similar across dates to
-- other episodes of the same show?
select
    te1.date as date1,
    te1.show_id as show_id1,

    te2.date as date2,
    te2.show_id as show_id2
from tmp_episodes te1
    inner join tmp_episodes te2 using(show_id)
where
    te1.date > te2.date;

drop table if exists datasets.show_pairs_sample;
create table datasets.show_pairs_sample as
select
    x.date1,
    x.show_id1,
    x.station_id1,

    x.date2,
    x.show_id2,
    x.station_id2,

    x.cnt1,
    x.duration1,
    x.show_confidence1,
    x.word_confidence1,
    x.is_public1,
    x.show_name1,
    x.twitter1::int as twitter1,

    x.cnt2,
    x.duration2,
    x.show_confidence2,
    x.word_confidence2,
    x.is_public2,
    x.show_name2,
    x.twitter2::int as twitter2,

    (x.date1 = x.date2)::int as within_day,
    (x.show_id1 = x.show_id2)::int as within_show,
    x.twitter1::int + x.twitter2::int as twitter_count
from
(
    select
        ts.date1,
        ts.show_id1,

        ts.date2,
        ts.show_id2,

        te1.station_id as station_id1,
        te1.cnt as cnt1,
        te1.duration as duration1,
        te1.show_confidence as show_confidence1,
        te1.word_confidence as word_confidence1,
        te1.is_public as is_public1,
        te1.show_name as show_name1,
        te1.twitter as twitter1,

        te2.station_id as station_id2,
        te2.cnt as cnt2,
        te2.duration as duration2,
        te2.show_confidence as show_confidence2,
        te2.word_confidence as word_confidence2,
        te2.is_public as is_public2,
        te2.show_name as show_name2,
        te2.twitter as twitter2,

        row_number() over (
            partition by
                te1.twitter,
                te2.twitter,

                ts.date1 = ts.date2,

                -- both "when" branches need a common type,
                -- so coercing to character is a hack
                case ts.date1 = ts.date2
                    when true then ts.date1::varchar
                    else ts.show_id1::varchar
                end
            order by random()
        ) as rn
    from tmp_samples ts
        inner join tmp_episodes te1 on
            te1.date = ts.date1 and
            te1.show_id = ts.show_id1
        inner join tmp_episodes te2 on
            te2.date = ts.date2 and
            te2.show_id = ts.show_id2
) x
where
    (x.twitter1 and x.twitter2) or

    -- a random sample of other shows to compare to
    (x.twitter1 and not x.twitter2 and x.rn <= 50) or
    (not x.twitter1 and x.twitter2 and x.rn <= 50) or
    (not x.twitter1 and not x.twitter2 and x.rn <= 100);

select
    *
from datasets.show_pairs_sample;

