/*
 * Census geo <=> radio geo crosswalk logic
 */

drop table if exists tmp_census;
create local temporary table tmp_census
on commit preserve rows as
select
    cg.geography_id,

    case
        when ST_isValid(ST_GeomFromText(cg.wkt, 4326))
            then ST_GeomFromText(cg.wkt, 4326)
        else ST_ConvexHull(ST_GeomFromText(cg.wkt, 4326))
    end as geom
from census.geography cg
    inner join census.survey su on su.survey_id = cg.defining_survey_id
where
    su.is_census and
    su.year = 2010 and

    -- both tracts and block groups (which have a tract code)
    cg.tract_code is not null;
create index tmp_census_geom_index on tmp_census using gist (geom);

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

drop table if exists datasets.census_radio_crosswalk;
create table datasets.census_radio_crosswalk as
select
    x.*,

    case x.station_contains_census_geo
        when true then 1
        else x.overlap_area / x.census_area
    end as station_map_frac_of_census_geo
from
(
    select
        ts.station_map_id,
        tc.geography_id,

        ST_Contains(ts.geom, tc.geom) as station_contains_census_geo,
        case ST_Contains(ts.geom, tc.geom)
            when true then null
            when false then ST_Intersection(ts.geom, tc.geom)
        end as overlap_geom,

        ST_Area(ts.geom) as station_area,
        ST_Area(tc.geom) as census_area,
        ST_Area(case -- an efficiency hack
            when ST_Contains(ts.geom, tc.geom) then tc.geom
            else ST_Intersection(ts.geom, tc.geom)
        end) as overlap_area
    from tmp_station_maps ts
        inner join tmp_census tc on ST_Intersects(ts.geom, tc.geom)
) x;

create index census_radio_crosswalk_station_map_id_index
on datasets.census_radio_crosswalk
using btree (station_map_id);

create index census_radio_crosswalk_geography_id_index
on datasets.census_radio_crosswalk
using btree (geography_id);

