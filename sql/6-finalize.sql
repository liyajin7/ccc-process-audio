/*
 * Finalize: enable constraints, create indexes
 */

begin;
    --
    -- PK and FK constraints
    --

    alter table radio.snippet_syndication
    add primary key (syndication_method, snippet_id)
    deferrable initially immediate;

    alter table radio.snippet_syndication
    add constraint snippet_syndication_snippet_id_fkey
    foreign key (snippet_id)
    references radio.snippet (snippet_id)
    deferrable initially immediate;

    alter table radio.snippet
    add constraint snippet_station_id_fkey
    foreign key (station_id)
    references radio.station (station_id)
    deferrable initially immediate;

    alter table radio.snippet
    add constraint snippet_show_id_fkey
    foreign key (show_id)
    references radio.show (show_id)
    deferrable initially immediate;

    --
    -- Indexes
    --

    create index on radio.snippet using btree (date, show_id, station_id);
    create index on radio.snippet using btree (start_dt, station_id);
    create index on radio.snippet using btree (station_id, start_dt);
    create index on radio.snippet using btree (show_id, start_dt);
    create index on radio.snippet using btree (start_dt, show_id);

    create index
    on radio.snippet_syndication
    using btree
    (syndication_method, syndicated, snippet_id);

    --
    -- Statistics
    --

    analyze radio.snippet;
    analyze radio.snippet_syndication;

    --
    -- A view for common queries
    --

    drop view if exists radio.snippet_data;
    create view radio.snippet_data as
    select
        sn.*,

        st.format as station_format,
        st.band as station_band,
        st.std_owner as station_std_owner,
        (st.format = 'Public Radio') as is_public,

        ds.postal_code as station_state,
        ds.census_region_5way as station_census_region,

        sh.name as show_name,

        ss.syndicated as syndicated_show,
        sa.syndicated as syndicated_approxhash
    from radio.snippet sn
        inner join radio.station st using(station_id)
        inner join dim.state ds using(state_id)
        left join radio.show sh using(show_id)

        left join
        (
            select
                ssi.snippet_id,
                ssi.syndicated
            from radio.snippet_syndication ssi
            where
                ssi.syndication_method = 'show'
        ) ss on ss.snippet_id = sn.snippet_id

        left join
        (
            select
                ssi.snippet_id,
                ssi.syndicated
            from radio.snippet_syndication ssi
            where
                ssi.syndication_method = 'approxhash'
        ) sa on sa.snippet_id = sn.snippet_id
    where
        not st.exclude and
        not coalesce(sh.exclude, false) and

        sn.content <> '';
commit;

