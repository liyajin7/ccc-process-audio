drop table if exists datasets.radio_top_word_counts;
create table datasets.radio_top_word_counts as
select
    w.show_id,
    w.year,
    w.month,
    w.word,

    w.cnt
from
(
    select
        sn.show_id,
        extract(year from sn.start_dt) as year,
        extract(month from sn.start_dt) as month,
        regexp_split_to_table(lower(sn.content), '\s') as word,

        count(*) as cnt,

        row_number() over (
            partition by
                sn.show_id,
                extract(year from sn.start_dt),
                extract(month from sn.start_dt)
            order by
                count(*) desc
        ) as rn
    from radio.snippet sn
    where
        sn.show_id is not null
    group by 1,2,3,4
) w
where
    w.rn <= 1000;

