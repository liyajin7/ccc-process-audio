drop table if exists datasets.votes_by_station_map;
create table datasets.votes_by_station_map as
select
    rc.station_map_id,

    count(distinct pr.precinct_id) as nprecincts,

    sum(case rc.station_contains_precinct
        when true then pr.votes_pres_dem
        else (rc.overlap_area / rc.precinct_area) * pr.votes_pres_dem
    end) as votes_pres_dem,

    sum(case rc.station_contains_precinct
        when true then pr.votes_pres_rep
        else (rc.overlap_area / rc.precinct_area) * pr.votes_pres_rep
    end) as votes_pres_rep,

    sum(case rc.station_contains_precinct
        when true then pr.votes_pres_total
        else (rc.overlap_area / rc.precinct_area) * pr.votes_pres_total
    end) as votes_pres_total
from datasets.precinct_radio_crosswalk rc
    inner join election.precinct_results_2016 pr using(precinct_id)
group by 1;

create index votes_by_station_map_station_map_id_index
on datasets.votes_by_station_map
using btree (station_map_id);

drop table if exists tmp_votes_by_station;
create local temporary table tmp_votes_by_station
on commit preserve rows as
select
    sm.station_id,

    avg(dv.votes_pres_dem) as votes_pres_dem,
    avg(dv.votes_pres_rep) as votes_pres_rep,
    avg(dv.votes_pres_total) as votes_pres_total
from radio.station_map sm
    inner join datasets.votes_by_station_map dv using(station_map_id)
where
    sm.strength = 'moderate' and
    sm.time_of_day in ('A', 'D', 'N')
group by 1;

drop table if exists datasets.votes_by_show;
create table datasets.votes_by_show as
select
    ssf.show_id,

    sum(ssf.station_frac_of_show * vs.votes_pres_dem) as votes_pres_dem,
    sum(ssf.station_frac_of_show * vs.votes_pres_rep) as votes_pres_rep,
    sum(ssf.station_frac_of_show * vs.votes_pres_total) as votes_pres_total
from datasets.show_station_fractions ssf
    inner join tmp_votes_by_station vs using(station_id)
group by 1;

