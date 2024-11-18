-- don't use snippet_data: snippets with no content still have duration
create or replace temporary view tmp_eligible_snippets as
select
    sn.*
from radio.snippet sn
    inner join radio.station st using(station_id)
    left join radio.show sh using(show_id)
where
    not st.exclude and
    not coalesce(sh.exclude, false);

--
-- Outputs
--

select
    count(distinct us.user_id) as users,
    count(distinct us.show_id) as shows_with_twitter
from twitter.user_show us
where
    us.show_id in
    (
        select
            tes.show_id
        from tmp_eligible_snippets tes
        where
            tes.show_id is not null
    );

select
    count(distinct sn.station_id) as stations,
    count(distinct sn.show_id) as shows,
    sum(sn.duration) as seconds,
    sum(sn.wordcount) as words,

    count(sh.show_id) / count(sn.show_id)::float as twitter_share_of_show_id_snippets,

    sum(case sh.show_id is not null
        when true then sn.wordcount
        else 0
    end) / sum(case sn.show_id is not null
        when true then sn.wordcount
        else 0
    end)::float as twitter_share_of_show_id_wc,

    count(sn.show_id) / count(*)::float as sn_fraction_with_show_id,
    count(sh.show_id) / count(*)::float as sn_fraction_with_twitter,

    sum(case sn.show_id is not null
        when true then sn.wordcount
        else 0
    end) / sum(sn.wordcount)::float as wc_fraction_with_show_id,

    sum(case sh.show_id is not null
        when true then sn.wordcount
        else 0
    end) / sum(sn.wordcount)::float as wc_fraction_with_twitter,

    sum(case sn.show_id is not null
        when true then sn.duration
        else 0
    end) / sum(sn.duration)::float as dr_fraction_with_show_id,

    sum(case sh.show_id is not null
        when true then sn.duration
        else 0
    end) / sum(sn.duration)::float as dr_fraction_with_twitter
from tmp_eligible_snippets sn
    left join
    (
        select distinct
            us.show_id
        from twitter.user_show us
    ) sh on sh.show_id = sn.show_id;

select
    extract(year from sn.start_dt) as year,

    count(distinct sn.station_id) as stations,
    count(distinct sn.show_id) as shows,
    sum(sn.duration) as seconds,
    sum(sn.wordcount) as words,

    count(sh.show_id) / count(sn.show_id)::float as twitter_share_of_show_id,

    sum(case sh.show_id is not null
        when true then sn.wordcount
        else 0
    end) / sum(case sn.show_id is not null
        when true then sn.wordcount
        else 0
    end)::float as twitter_share_of_show_id_wc,

    count(sn.show_id) / count(*)::float as sn_fraction_with_show_id,
    count(sh.show_id) / count(*)::float as sn_fraction_with_twitter,

    sum(case sn.show_id is not null
        when true then sn.wordcount
        else 0
    end) / sum(sn.wordcount)::float as wc_fraction_with_show_id,

    sum(case sh.show_id is not null
        when true then sn.wordcount
        else 0
    end) / sum(sn.wordcount)::float as wc_fraction_with_twitter,

    sum(case sn.show_id is not null
        when true then sn.duration
        else 0
    end) / sum(sn.duration)::float as dr_fraction_with_show_id,

    sum(case sh.show_id is not null
        when true then sn.duration
        else 0
    end) / sum(sn.duration)::float as dr_fraction_with_twitter
from tmp_eligible_snippets sn
    left join
    (
        select distinct
            us.show_id
        from twitter.user_show us
    ) sh on sh.show_id = sn.show_id
group by 1;

