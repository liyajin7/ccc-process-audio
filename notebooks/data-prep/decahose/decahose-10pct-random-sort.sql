/*
 * Run this file in Athena against the socmac AWS account; save the results to data/paper-round-3/decahose/decahose-10pct-random-sort-20230509.csv
 */

with base as
(
    select
        id,
        actor_id as user_id,
        posted_time as postedtime,
        verb, -- 'post' = original tweet, 'share' = retweet

        retweet_count,
        object_favorites_count as favorites_count,

        actor_preferred_username as username,
        actor_friends_count as friends_count,
        actor_followers_count as followers_count,
        actor_statuses_count as statuses_count,
        actor_verified as verified,

        body
    from decahose.semiparsed
    where
        twitter_lang = 'en' and
        (
            (
                day >= cast('2019-09-01' as date) and
                day <= cast('2019-11-01' as date)
            )

            or

            (
                day >= cast('2020-03-01' as date) and
                day <= cast('2020-05-01' as date)
            )

            or

            (
                day >= cast('2021-01-01' as date) and
                day <= cast('2021-03-01' as date)
            )

            or

            (
                day >= cast('2022-03-01' as date) and
                day <= cast('2022-05-11' as date)
            )
        )
),

samp as
(
    select
        *
    from base
    tablesample bernoulli(10)
)

select
    *
from samp
order by random();
