select
    sn.date,
    sn.show_id,
    sn.station_id,

    count(*) as cnt,
    sum(sn.duration) as duration
from radio.snippet_data sn
where
    sn.show_id is not null
group by 1,2,3;

