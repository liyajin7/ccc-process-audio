/*
 * Election geo <=> radio geo crosswalk logic
 */

drop table if exists tmp_precincts;
create local temporary table tmp_precincts
on commit preserve rows as
select
    pr.precinct_id,

    case
        when ST_isValid(ST_GeomFromText(pr.wkt, 4326))
            then ST_GeomFromText(pr.wkt, 4326)
        else ST_ConvexHull(ST_GeomFromText(pr.wkt, 4326))
    end as geom
from election.precinct_results_2016 pr;
create index tmp_precincts_geom_index on tmp_precincts using gist (geom);

drop table if exists tmp_station_maps;
create local temporary table tmp_station_maps
on commit preserve rows as
select
    sm.station_map_id,
    sm.time_of_day,
    sm.strength,

    ST_GeomFromText(sm.wkt, 4326) as geom
from radio.station_map sm;
create index tmp_stations_geom_index on tmp_station_maps using gist (geom);

drop table if exists datasets.precinct_radio_crosswalk;
create table datasets.precinct_radio_crosswalk as
select
    x.*,

    case x.station_contains_precinct
        when true then 1
        else x.overlap_area / x.precinct_area
    end as station_map_frac_of_precinct
from
(
    select
        ts.station_map_id,
        pr.precinct_id,

        ST_Contains(ts.geom, pr.geom) as station_contains_precinct,
        case ST_Contains(ts.geom, pr.geom)
            when true then null
            when false then ST_Intersection(ts.geom, pr.geom)
        end as overlap_geom,

        ST_Area(ts.geom) as station_area,
        ST_Area(pr.geom) as precinct_area,
        ST_Area(case -- an efficiency hack
            when ST_Contains(ts.geom, pr.geom) then pr.geom
            else ST_Intersection(ts.geom, pr.geom)
        end) as overlap_area
    from tmp_station_maps ts
        inner join tmp_precincts pr on ST_Intersects(ts.geom, pr.geom)
) x;

create index precinct_radio_crosswalk_station_map_id_index
on datasets.precinct_radio_crosswalk
using btree (station_map_id);

create index precinct_radio_crosswalk_precinct_id_index
on datasets.precinct_radio_crosswalk
using btree (precinct_id);

