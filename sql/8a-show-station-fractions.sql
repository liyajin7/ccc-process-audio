drop table if exists datasets.show_station_fractions;
create table datasets.show_station_fractions as
select
    x.show_id,
    x.station_id,

    x.cnt as cnt_overlap,
    sum(x.cnt) over (partition by x.show_id) as cnt_show,

    x.cnt / sum(x.cnt) over (partition by x.show_id) as station_frac_of_show
from
(
    select
        sn.show_id,
        sn.station_id,

        count(*) as cnt
    from radio.snippet sn
        inner join radio.station st using(station_id)
        inner join radio.show sh using(show_id)
    where
        not st.exclude and
        not sh.exclude
    group by 1,2
) x;

