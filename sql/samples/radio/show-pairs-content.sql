with tmp_targets as
(
    select
        sps1.date1 as date,
        sps1.show_id1 as show_id,
        sps1.station_id1 as station_id
    from datasets.show_pairs_sample sps1

    union

    select
        sps2.date2 as date,
        sps2.show_id2 as show_id,
        sps2.station_id2 as station_id
    from datasets.show_pairs_sample sps2
)

select
    sn.date,
    sn.show_id,
    sn.station_id,

    -- dummy aggregate
    bool_or(sn.is_public) as is_public,

    -- real aggregate
    string_agg(sn.content, ' ') as content
from radio.snippet_data sn
    inner join tmp_targets tt using(date, show_id, station_id)
group by 1,2,3;

