select
    sn.station_std_owner,

    count(*) as nsnippets,
    sum(sn.duration) as nseconds,
    count(distinct sn.station_id) as nstations,
    count(distinct sn.show_id) as nshows,

    avg(sn.is_public::int) as public_frac
from radio.snippet_data sn
group by 1
order by 4 desc;

