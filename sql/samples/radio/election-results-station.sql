select
    sm.station_map_id,
    sm.station_id,
    sm.time_of_day,
    sm.strength,

    dv.votes_pres_dem,
    dv.votes_pres_rep,
    dv.votes_pres_total,

    dv.votes_pres_dem / dv.votes_pres_total as d_share,
    dv.votes_pres_dem / (dv.votes_pres_dem + dv.votes_pres_rep) as d_share_2way
from radio.station_map sm
    inner join datasets.votes_by_station_map dv using(station_map_id);

