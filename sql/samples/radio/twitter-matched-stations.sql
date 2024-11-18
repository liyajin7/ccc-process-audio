-- this query produces the list of stations in the Twitter-matched sample as
-- included in the paper supplementary information
with tmp_stations as
(
    select distinct
        st.station_id
    from radio.station st
        inner join radio.snippet sn using(station_id)
        inner join radio.show sh using(show_id)
        inner join twitter.user_show us using(show_id)
    where
        not st.exclude and
        not sh.exclude
)
select
    st.station_id,
    st.callsign,
    st.band,
    st.freq,
    st.format,
    s.postal_code
from radio.station st
    inner join dim.state s using(state_id)
    inner join tmp_stations ts using(station_id);

