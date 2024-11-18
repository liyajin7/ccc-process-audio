/*
 * Station and show dim data
 */

begin;
    set constraints all deferred;

    insert into radio.show
        (show_id, parent_show_id, name, is_music, is_sports, exclude)
    select
        sst.show_id,
        nullif(sst.parent_show_id, 0),
        sst.name,
        sst.is_music,
        sst.is_sports,
        sst.exclude
    from stg.show_twitter sst;

    insert into radio.station
        (station_id, state_id, callsign, band, freq, is_licensed, format, raw_owner,
        std_owner, ingested, exclude)
    select
        ss.station_id,
        ds.state_id,

        ss.callsign,
        ss.band,
        ss.freq,
        ss.is_licensed,
        ss.format,
        ss.owner,
        sso.std_owner,

        scm.station_id is not null,
        case scm.station_id is not null
            when true then sso.exclude
            else null
        end
    from stg.station ss
        left join dim.state ds on ds.postal_code = ss.state
        left join stg.callsign_map scm on scm.station_id = ss.station_id
        left join stg.station_ownership sso on sso.station_id = ss.station_id;

    insert into radio.station_map
        (station_id, time_of_day, strength, transmitter_latitude,
         transmitter_longitude, wkt)
    select
        ssm.station_id,
        ssm.time_of_day,
        smc.strength,

        max(ssm.latitude) as transmitter_latitude,
        max(ssm.longitude) as transmitter_longitude,

        st_astext(st_convexhull(st_collect(st_setsrid(st_point(smc.longitude, smc.latitude), 4326)))) as geom
    from stg.station_map ssm
        inner join stg.station_map_coordinate smc using(station_map_id)
    group by 1,2,3;

    set constraints all immediate;

    analyze radio.show;
    analyze radio.station;
    analyze radio.station_map;
commit;

