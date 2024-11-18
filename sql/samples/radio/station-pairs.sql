drop table if exists tmp_station_date;
create local temporary table tmp_station_date
on commit preserve rows as
select
    sn.date,
    sn.station_id,

    count(*) as cnt
from radio.snippet_data sn
group by 1,2;

drop table if exists datasets.station_pairs_sample;
create table datasets.station_pairs_sample as
select
    x.date,
    x.station_id1,
    x.station_id2,

    x.cnt1,
    x.cnt2
from
(
    select
        ts1.date,
        ts1.station_id as station_id1,
        ts2.station_id as station_id2,

        ts1.cnt as cnt1,
        ts2.cnt as cnt2,

        row_number() over (partition by ts1.date order by random()) as rn
    from tmp_station_date ts1
        inner join tmp_station_date ts2 using(date)
    where
        ts1.station_id > ts2.station_id
) x
where
    x.rn <= 200;

select
    *
from datasets.station_pairs_sample;

