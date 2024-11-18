/*
 * Map variables to station map
 */

drop table if exists tmp_census;
create local temporary table tmp_census
on commit preserve rows as
select
    coalesce(bg.geography_id, tr.geography_id) as geography_id,
    coalesce(bg.geography_type, tr.geography_type) as geography_type,
    coalesce(bg.population, tr.population) as population,

    coalesce(bg.n_male, 0) as n_male,
    coalesce(bg.n_white, 0) as n_white,
    coalesce(bg.n_black, 0) as n_black,
    coalesce(bg.n_native, 0) as n_native,
    coalesce(bg.n_asian, 0) as n_asian,
    coalesce(bg.n_multiracial, 0) as n_multiracial,
    coalesce(bg.n_hispanic, 0) as n_hispanic,
    coalesce(bg.n_drive_alone, 0) as n_drive_alone,
    coalesce(bg.n_commute_90plus, 0) as n_commute_90plus,
    coalesce(bg.n_commute_60plus, 0) as n_commute_60plus,
    coalesce(bg.n_renting, 0) as n_renting,
    coalesce(bg.n_in_labor_force, 0) as n_in_labor_force,
    coalesce(bg.n_college_graduate, 0) as n_college_graduate,
    coalesce(bg.median_male_age, 0) as median_male_age,
    coalesce(bg.median_female_age, 0) as median_female_age,
    coalesce(bg.median_rent, 0) as median_rent,
    coalesce(bg.median_hh_income, 0) as median_hh_income,
    coalesce(tr.n_children, 0) as n_children,
    coalesce(tr.n_immigrants, 0) as n_immigrants,
    coalesce(tr.mean_hours_worked, 0) as mean_hours_worked
from
(
    select
        cg.geography_id,
        'blockgroup' as geography_type,

        sum(case -- total population
            when cv.name = 'B01003e1' then cd.value::int
            else 0
        end) as population,

        sum(case -- number of men and boys
            when cv.name = 'B01001e2' then cd.value::int
            else 0
        end) as n_male,

        sum(case -- number white
            when cv.name = 'B02001e2' then cd.value::int
            else 0
        end) as n_white,

        sum(case -- number black
            when cv.name = 'B02001e3' then cd.value::int
            else 0
        end) as n_black,

        sum(case -- number native
            when cv.name = 'B02001e4' then cd.value::int
            else 0
        end) as n_native,

        sum(case -- number asian
            when cv.name = 'B02001e5' then cd.value::int
            else 0
        end) as n_asian,

        sum(case -- number other or 2+
            when cv.name = 'B02001e6' then cd.value::int
            when cv.name = 'B02001e7' then cd.value::int
            when cv.name = 'B02001e8' then cd.value::int
            else 0
        end) as n_multiracial,

        sum(case -- number hispanic (not a race)
            when cv.name = 'B03003e3' then cd.value::int
            else 0
        end) as n_hispanic,

        sum(case -- drove alone to work
            when cv.name = 'B08301e3' then cd.value::int
            else 0
        end) as n_drive_alone,

        sum(case -- number 90+ minute commute
            when cv.name = 'B08303e13' then cd.value::int
            else 0
        end) as n_commute_90plus,

        sum(case -- number 60+ minute commute
            when cv.name = 'B08303e12' then cd.value::int
            when cv.name = 'B08303e13' then cd.value::int
            else 0
        end) as n_commute_60plus,

        sum(case -- number renting
            when cv.name = 'B25008e3' then cd.value::int
            else 0
        end) as n_renting,

        sum(case -- number in labor force
            when cv.name = 'B23025e2' then cd.value::int
            else 0
        end) as n_in_labor_force,

        sum(case -- number bachelors+
            when cv.name = 'B15003e25' then cd.value::int
            when cv.name = 'B15003e24' then cd.value::int
            when cv.name = 'B15003e23' then cd.value::int
            when cv.name = 'B15003e22' then cd.value::int
            else 0
        end) as n_college_graduate,

        sum(case -- median male age
            when cv.name = 'B01002e2' then cd.value::float
            else 0
        end) as median_male_age,

        sum(case -- median female age
            when cv.name = 'B01002e3' then cd.value::float
            else 0
        end) as median_female_age,

        sum(case -- median rent
            when cv.name = 'B25064e1' then cd.value::float
            else 0
        end) as median_rent,

        sum(case -- median household income
            when cv.name = 'B19013e1' then cd.value::float
            else 0
        end) as median_hh_income
    from census.geography cg
        inner join census.data cd using(geography_id)
        inner join census.variable cv using(variable_id)
    where
        cg.block_group_code is not null
    group by 1
) bg

full join

(
    select
        cg.geography_id,
        'tract' as geography_type,

        sum(case -- total population
            when cv.name = 'B01003e1' then cd.value::int
            else 0
        end) as population,

        sum(case -- number of children
            when cv.name = 'B09001e1' then cd.value::int
            else 0
        end) as n_children,

        sum(case -- number immigrants (naturalized + noncitizen)
            when cv.name = 'B05011e1' then cd.value::int
            else 0
        end) as n_immigrants,

        sum(case -- mean hours worked
            when cv.name = 'B23020e1' then cd.value::float
            else 0
        end) as mean_hours_worked
    from census.geography cg
        inner join census.data cd using(geography_id)
        inner join census.variable cv using(variable_id)
    where
        cg.tract_code is not null and
        cg.block_group_code is null
    group by 1
) tr using(geography_id);
create index tmp_census_idx on tmp_census using btree (geography_id);

drop table if exists datasets.census_by_station_map;
create table datasets.census_by_station_map as
select
    rc.station_map_id,

    count(distinct rc.geography_id) as ngeos,
    sum(case
        when tp.geography_type = 'blockgroup'
            then rc.station_map_frac_of_census_geo * tp.population
        else 0
    end) as population,

    --
    -- sum up counts
    --
    sum(rc.station_map_frac_of_census_geo * tp.n_children) as n_children,
    sum(rc.station_map_frac_of_census_geo * tp.n_male) as n_male,
    sum(rc.station_map_frac_of_census_geo * tp.n_white) as n_white,
    sum(rc.station_map_frac_of_census_geo * tp.n_black) as n_black,
    sum(rc.station_map_frac_of_census_geo * tp.n_native) as n_native,
    sum(rc.station_map_frac_of_census_geo * tp.n_asian) as n_asian,
    sum(rc.station_map_frac_of_census_geo * tp.n_multiracial) as n_multiracial,
    sum(rc.station_map_frac_of_census_geo * tp.n_hispanic) as n_hispanic,
    sum(rc.station_map_frac_of_census_geo * tp.n_drive_alone) as n_drive_alone,
    sum(rc.station_map_frac_of_census_geo * tp.n_commute_90plus) as n_commute_90plus,
    sum(rc.station_map_frac_of_census_geo * tp.n_commute_60plus) as n_commute_60plus,
    sum(rc.station_map_frac_of_census_geo * tp.n_renting) as n_renting,
    sum(rc.station_map_frac_of_census_geo * tp.n_in_labor_force) as n_in_labor_force,
    sum(rc.station_map_frac_of_census_geo * tp.n_immigrants) as n_immigrants,
    sum(rc.station_map_frac_of_census_geo * tp.n_college_graduate) as n_college_graduate,

    --
    -- average non-counts, weighted by population
    --
    sum(rc.station_map_frac_of_census_geo * tp.population * tp.median_male_age) /
    sum(rc.station_map_frac_of_census_geo * tp.population) as median_male_age,

    sum(rc.station_map_frac_of_census_geo * tp.population * tp.median_female_age) /
    sum(rc.station_map_frac_of_census_geo * tp.population) as median_female_age,

    sum(rc.station_map_frac_of_census_geo * tp.population * tp.median_rent) /
    sum(rc.station_map_frac_of_census_geo * tp.population) as median_rent,

    sum(rc.station_map_frac_of_census_geo * tp.population * tp.median_hh_income) /
    sum(rc.station_map_frac_of_census_geo * tp.population) as median_hh_income,

    sum(rc.station_map_frac_of_census_geo * tp.population * tp.mean_hours_worked) /
    sum(rc.station_map_frac_of_census_geo * tp.population) as mean_hours_worked
from datasets.census_radio_crosswalk rc
    inner join tmp_census tp using(geography_id)
group by 1;

create index census_by_station_map_station_map_id_index
on datasets.census_by_station_map
using btree (station_map_id);

drop table if exists datasets.population_by_station_map;
create table datasets.population_by_station_map as
select
    station_map_id,
    ngeos,
    population
from datasets.census_by_station_map;

create index population_by_station_map_station_map_id_index
on datasets.population_by_station_map
using btree (station_map_id);

/*
 * Map variables to show
 */

drop table if exists tmp_census_by_station;
create local temporary table tmp_census_by_station
on commit preserve rows as
select
    sm.station_id,

    -- a station will either have one A row or a D and N row; this avg() takes
    -- the value of the A row if there is one, or otherwise the average of the
    -- D and N. this is basically equivalent to assuming the D map is good for
    -- 12 hours of the day and the N map for the other 12, and weighting by the
    -- proportion of the day each map is on.
    avg(dv.population) as population,
    avg(dv.n_children) as n_children,
    avg(dv.n_male) as n_male,
    avg(dv.n_white) as n_white,
    avg(dv.n_black) as n_black,
    avg(dv.n_native) as n_native,
    avg(dv.n_asian) as n_asian,
    avg(dv.n_multiracial) as n_multiracial,
    avg(dv.n_hispanic) as n_hispanic,
    avg(dv.n_drive_alone) as n_drive_alone,
    avg(dv.n_commute_90plus) as n_commute_90plus,
    avg(dv.n_commute_60plus) as n_commute_60plus,
    avg(dv.n_renting) as n_renting,
    avg(dv.n_in_labor_force) as n_in_labor_force,
    avg(dv.n_immigrants) as n_immigrants,
    avg(dv.n_college_graduate) as n_college_graduate,
    avg(dv.median_male_age) as median_male_age,
    avg(dv.median_female_age) as median_female_age,
    avg(dv.median_rent) as median_rent,
    avg(dv.median_hh_income) as median_hh_income,
    avg(dv.mean_hours_worked) as mean_hours_worked
from radio.station_map sm
    inner join datasets.census_by_station_map dv using(station_map_id)
where
    sm.strength = 'moderate' and
    sm.time_of_day in ('A', 'D', 'N')
group by 1;

drop table if exists datasets.census_by_show;
create table datasets.census_by_show as
select
    ssf.show_id,

    -- sum(ssf.station_frac_of_show) = 1 => convex combination
    sum(ssf.station_frac_of_show * dv.population) as population,

    --
    -- turn counts into population fractions
    --
    sum(ssf.station_frac_of_show * dv.n_children) /
    sum(ssf.station_frac_of_show * dv.population) as frac_children,

    sum(ssf.station_frac_of_show * dv.n_male) /
    sum(ssf.station_frac_of_show * dv.population) as frac_male,

    sum(ssf.station_frac_of_show * dv.n_white) /
    sum(ssf.station_frac_of_show * dv.population) as frac_white,

    sum(ssf.station_frac_of_show * dv.n_black) /
    sum(ssf.station_frac_of_show * dv.population) as frac_black,

    sum(ssf.station_frac_of_show * dv.n_native) /
    sum(ssf.station_frac_of_show * dv.population) as frac_native,

    sum(ssf.station_frac_of_show * dv.n_asian) /
    sum(ssf.station_frac_of_show * dv.population) as frac_asian,

    sum(ssf.station_frac_of_show * dv.n_multiracial) /
    sum(ssf.station_frac_of_show * dv.population) as frac_multiracial,

    sum(ssf.station_frac_of_show * dv.n_hispanic) /
    sum(ssf.station_frac_of_show * dv.population) as frac_hispanic,

    sum(ssf.station_frac_of_show * dv.n_drive_alone) /
    sum(ssf.station_frac_of_show * dv.population) as frac_drive_alone,

    sum(ssf.station_frac_of_show * dv.n_commute_90plus) /
    sum(ssf.station_frac_of_show * dv.population) as frac_commute_90plus,

    sum(ssf.station_frac_of_show * dv.n_commute_60plus) /
    sum(ssf.station_frac_of_show * dv.population) as frac_commute_60plus,

    sum(ssf.station_frac_of_show * dv.n_renting) /
    sum(ssf.station_frac_of_show * dv.population) as frac_renting,

    sum(ssf.station_frac_of_show * dv.n_in_labor_force) /
    sum(ssf.station_frac_of_show * dv.population) as frac_in_labor_force,

    sum(ssf.station_frac_of_show * dv.n_immigrants) /
    sum(ssf.station_frac_of_show * dv.population) as frac_immigrants,

    sum(ssf.station_frac_of_show * dv.n_college_graduate) /
    sum(ssf.station_frac_of_show * dv.population) as frac_college_graduate,

    --
    -- no need to divide by population here
    --
    sum(ssf.station_frac_of_show * dv.median_male_age) as median_male_age,
    sum(ssf.station_frac_of_show * dv.median_female_age) as median_female_age,
    sum(ssf.station_frac_of_show * dv.median_rent) as median_rent,
    sum(ssf.station_frac_of_show * dv.median_hh_income) as median_hh_income,
    sum(ssf.station_frac_of_show * dv.mean_hours_worked) as mean_hours_worked
from datasets.show_station_fractions ssf
    inner join tmp_census_by_station dv using(station_id)
group by 1;

