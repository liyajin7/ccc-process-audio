select
    x.station_id,
    x.time_of_day,
    x.strength,
    x.wkt
from
(
    select
        sm.station_id,
        sm.time_of_day,
        sm.strength,
        sm.wkt,

        row_number() over (
            partition by sm.station_id
            order by sm.time_of_day asc -- note already A > D > N > ...
        ) as rn
    from radio.station_map sm
    where
        sm.strength = 'moderate' and
        sm.station_id in
        (
            select
                sn.station_id
            from radio.snippet sn
            where
                sn.station_id is not null
        )
) x
where
    x.rn = 1;

