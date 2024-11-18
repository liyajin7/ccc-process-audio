/*
 * 2016 ACS data
 */

begin;
    set constraints all deferred;

    insert into census.variable
        (name, description)
    select
        cv.variable,
        cv.description
    from stg.census_variable cv;

    insert into census.geography
        (defining_survey_id, geoid, name, state_fips, county_fips,
         tract_code, block_group_code, land_area, water_area, wkt)
    select
        (
            select
                cs.survey_id
            from census.survey cs
            where
                cs.name = '2010 Census'
        ),

        cg.geoid,
        cg.name,
        cg.state_fips,
        cg.county_fips,
        cg.tract_code,
        cg.block_group_code,
        cg.land_area,
        cg.water_area,
        cg.wkt
    from stg.census_geo cg;

    /*
     * This CTAS-truncate-insert three-step is much faster than
     * DELETE on postgres if you don't care about MVCC safety
     */

    drop table if exists tmp_census_data;
    create local temporary table tmp_census_data
    on commit preserve rows as
    select
        geoid,
        variable,
        value
    from stg.census_data cd
        inner join stg.tmp_census_variables cv on cv.name = cd.variable;

    truncate table stg.census_data;

    insert into stg.census_data
        (geoid, variable, value)
    select
        geoid,
        variable,
        value
    from tmp_census_data;

    drop table tmp_census_data;

    --
    -- Back to the data loading
    --

    insert into census.data
        (survey_id, geography_id, variable_id, value)
    select
        (
            select
                cs.survey_id
            from census.survey cs
            where
                cs.name = '2016 ACS'
        ),

        cg.geography_id,
        cv.variable_id,

        cd.value
    from stg.census_data cd
        left join census.geography cg on cg.geoid = cd.geoid
        left join census.variable cv on cd.variable = cv.name;

    set constraints all immediate;

    -- improve query performance
    analyze census.survey;
    analyze census.variable;
    analyze census.geography;
    analyze census.data;

    -- recover space
    truncate table stg.census_variable;
    truncate table stg.census_data;
    truncate table stg.census_geo;
commit;

