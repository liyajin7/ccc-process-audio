with recorded_show_ids as
(
    select distinct
        sn.show_id
    from radio.snippet sn
        inner join radio.station st using(station_id)
        inner join radio.show sh using(show_id)
    where
        not st.exclude and
        not sh.exclude
),

radio_users as
(
    select distinct
        ut.user_id
    from twitter.user_tag ut
        inner join twitter.user_show us using(user_id)
        inner join recorded_show_ids rsi using(show_id)
    where
        ut.tag = 'radio'
),

universe_users as
(
    select
        ut.user_id
    from twitter.user_tag ut
    where
        ut.tag = 'universe'
),

tmp_last_follow_fetch_all as
(
    select
        -- we don't need this per-user because it's unique in this table
        x.follow_fetch_id
    from
    (
        select
            ff.follow_fetch_id,

            row_number() over (
                partition by ff.user_id, ff.is_friends
                order by ff.insert_dt desc -- most recent
            ) as rn
        from twitter.follow_fetch ff
    ) x
    where
        x.rn = 1
),

followers as
(
    select distinct -- need the distinct, we didn't fetch these users' edges
        fo.source_user_id as user_id
    from twitter.follow fo
        inner join tmp_last_follow_fetch_all tlf on tlf.follow_fetch_id = fo.follow_fetch_id
        inner join radio_users ru on ru.user_id = fo.target_user_id
)

select distinct
    fo.source_user_id,
    fo.target_user_id,

    coalesce(us.show_id, 0) as show_id
from twitter.follow fo
    inner join tmp_last_follow_fetch_all tlf on tlf.follow_fetch_id = fo.follow_fetch_id
    inner join followers fr on fr.user_id = fo.source_user_id

    -- we need this because some of the universe users are also in the radio
    -- followers group and thus we'll end up pulling in many many friends that
    -- we're not interested in
    inner join universe_users uu on uu.user_id = fo.target_user_id

    left join twitter.user_show us on us.user_id = fo.target_user_id
order by 2 asc;

