select
    sn.date,
    sn.show_id,
    sn.station_id,

    -- dummy aggregate
    bool_or(sn.is_public) as is_public,
    max(sn.show_name) as show_name,

    -- real aggregate
    string_agg(sn.content, ' ') as content
from radio.snippet_data sn
    inner join datasets.radio_best_episode_confidence_only tt using(date, show_id, station_id)
where
    not sn.syndicated_show
group by 1,2,3;

