/*
 * 2016 election results
 */

begin;
    set constraints all deferred;

    insert into election.precinct_results_2016
        (precinct_id, state_id, votes_pres_dem, votes_pres_rep,
         votes_pres_total, wkt)
    select
        er.precinct_id,
        ds.state_id,

        er.votes_pres_dem,
        er.votes_pres_rep,
        er.votes_pres_total,
        er.wkt
    from stg.election_results er
        left join dim.state ds on ds.fips = er.state_fips;

    set constraints all immediate;

    analyze election.precinct_results_2016;
    truncate table stg.election_results; -- recover space
commit;

