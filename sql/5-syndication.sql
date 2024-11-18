/*
 * Syndication detection
 */

begin;
    drop table if exists datasets.multi_station_shows;
    create table datasets.multi_station_shows as
    select
        sn.show_id
    from radio.snippet sn
    where
        sn.show_id is not null
    group by 1
    having count(distinct sn.station_id) > 1;
    alter table datasets.multi_station_shows add primary key (show_id);
    analyze datasets.multi_station_shows;

    -- i.e., every approxhash occurring on multiple stations
    -- during a given week. if the appearances on >1 station
    -- don't take place within the same week, they won't count.
    drop table if exists datasets.multi_station_approxhashes;
    create table datasets.multi_station_approxhashes as
    select distinct
        x.approxhash
    from
    (
        select
            extract(year from sn.start_dt) as year,
            extract(week from sn.start_dt) as week,
            sn.approxhash
        from radio.snippet sn
        where
            sn.approxhash is not null
        group by 1, 2, 3
        having count(distinct sn.station_id) > 1
    ) x;
    alter table datasets.multi_station_approxhashes add primary key (approxhash);
    analyze datasets.multi_station_approxhashes;

    insert into radio.snippet_syndication
        (snippet_id, syndication_method, syndicated)
    select
        sn.snippet_id,
        'show',
        tms.show_id is not null
    from radio.snippet sn
        left join datasets.multi_station_shows tms using(show_id)
    where
        sn.show_id is not null;

    insert into radio.snippet_syndication
        (snippet_id, syndication_method, syndicated)
    select
        sn.snippet_id,
        'approxhash',
        dh.approxhash is not null
    from radio.snippet sn
        left join datasets.multi_station_approxhashes dh using(approxhash);
commit;

