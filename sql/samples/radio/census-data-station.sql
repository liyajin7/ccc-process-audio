select
    sm.station_map_id,
    sm.station_id,
    sm.time_of_day,
    sm.strength,

    dp.population
from radio.station_map sm
    inner join datasets.population_by_station_map dp using(station_map_id);

