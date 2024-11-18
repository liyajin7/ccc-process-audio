/*
 * The follow graph between a) source: users who follow >= 2 radio users,
 * b) target: radio users. Used for ideology estimation.
 */

drop table if exists tmp_radio;
create local temporary table tmp_radio
on commit preserve rows as
select
    ut.user_id
from twitter.user_tag ut
where
    ut.tag = 'radio';

drop table if exists tmp_last_follow_fetch;
create local temporary table tmp_last_follow_fetch
on commit preserve rows as
select
    x.follow_fetch_id,

    x.user_id,
    x.is_friends,
    x.is_followers
from
(
    select
        ff.follow_fetch_id,

        ff.user_id,
        ff.is_friends,
        ff.is_followers,

        row_number() over (
            partition by ff.user_id, ff.is_friends
            order by ff.insert_dt desc -- most recent
        ) as rn
    from twitter.follow_fetch ff
        inner join tmp_radio tu using(user_id)
) x
where
    x.rn = 1;

drop table if exists tmp_radio_followers;
create local temporary table tmp_radio_followers
on commit preserve rows as
select
    source_user_id,
    target_user_id
from twitter.follow fl
    inner join tmp_last_follow_fetch ff using(follow_fetch_id)
where
    target_user_id in
    (
        select
            user_id
        from tmp_radio
    );

select
    source_user_id as source,
    target_user_id as target
from tmp_radio_followers trf
where
    source_user_id in
    (
        select
            source_user_id
        from tmp_radio_followers ti
        group by 1
        having count(distinct target_user_id) > 1
    );
