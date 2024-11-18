drop table if exists datasets.paper_round_3_snippets;
create table datasets.paper_round_3_snippets as
select
    sn.snippet_id,

    sn.start_dt as timestamp,

    case ds.census_region_5way
        when 'South' then 'S'
        when 'Northeast' then 'N'
        when 'West' then 'W'
        when 'Midwest' then 'M'
        when 'Pacific' then 'P'
        else null
    end as station_census_region,
    (st.format = 'Public Radio')::int as is_public,
    (st.band = 'AM')::int as am_band,
    ss.syndicated::int as syndicated,

    sn.content
from radio.snippet sn
    inner join datasets.radio_best_episode_confidence_only be on
        be.date = sn.date and
        be.show_id = sn.show_id and
        be.station_id = sn.station_id

    inner join radio.station st on st.station_id = sn.station_id
    inner join dim.state ds on ds.state_id = st.state_id
    left join radio.show sh on sh.show_id = sn.show_id

    left join
    (
        select
            ssi.snippet_id,
            ssi.syndicated
        from radio.snippet_syndication ssi
        where
            ssi.syndication_method = 'show'
    ) ss on ss.snippet_id = sn.snippet_id
where
    not st.exclude and
    not coalesce(sh.exclude, false) and
    sn.content <> '';
