with tmp_snippet as
(
    select
        sn.station_id,

        count(distinct sn.date) as ndates,
        count(*) as nsnippets,
        sum(sn.duration) as duration
    from radio.snippet_data sn
    group by 1
)

select
    st.station_id,

    ts.ndates,
    ts.nsnippets,
    ts.duration,

    ts.station_id is not null as in_sample,
    st.format,
    st.std_owner,
    st.band,
    st.format = 'Public Radio' as is_public,
    ds.postal_code as state,
    ds.census_region_5way as census_region
from radio.station st
    left join tmp_snippet ts on ts.station_id = st.station_id
    left join dim.state ds on ds.state_id = st.state_id;

