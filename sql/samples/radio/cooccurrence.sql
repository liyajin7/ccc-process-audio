drop table if exists tmp_station_show;
create local temporary table tmp_station_show
on commit preserve rows as
select
    sn.station_id,
    sn.show_id,
    date(sn.start_dt) as date,

    -- dummy aggregate
    bool_and(st.format = 'Public Radio') as is_public,

    -- real aggregate
    count(*) as snippets
from radio.snippet sn
    inner join radio.station st using(station_id)
    inner join radio.show sh using(show_id)
where
    not st.exclude and
    not sh.exclude
group by 1,2,3;

select
    tss1.show_id as show_id1,
    tss2.show_id as show_id2,

    count(distinct case
        when not tss1.is_public then tss1.station_id
        else null
    end) as ntalk,

    count(distinct case
        when tss1.is_public then tss1.station_id
        else null
    end) as npublic,

    count(distinct tss1.station_id) as nstations
from tmp_station_show tss1
    inner join tmp_station_show tss2 using(station_id)
where
    -- don't just avoid comparing shows to themselves, but also
    -- avoid representing every pair twice: (show_id1, show_id2) is enough
    -- without also having (show_id2, show_id1)
    tss1.show_id < tss2.show_id and

    -- no edge between show1 and show2 if they appear more than a week apart;
    -- this is because shows are rarely to never on a longer than weekly cadence
    -- and we'd like to avoid false positives from schedule changes
    abs(tss1.date - tss2.date) < 7
group by 1,2;

