select
    tw.tweet_id as id,
    tw.user_id,
    tw.create_dt as timestamp,
    tw.lang,
    tw.source,
    tw.truncated,
    tw.retweeted_status_id is not null as is_retweet,
    tw.in_reply_to_status_id is not null as is_reply,
    tw.quoted_status_id is not null as is_quote_tweet,
    tw.retweet_count,
    tw.favorite_count,
    case tw.source
        when 'Twitter for iPhone' then 'iPhone'
        when 'Twitter for Android' then 'Android'
        when 'Twitter Web App' then 'Desktop'
        when 'Twitter Web Client' then 'Desktop'
        when 'TweetDeck' then 'Desktop'
        else 'Other'
    end as source_collapsed,

    replace(case
        when (tw.api_response::jsonb) ? 'retweeted_status'
        then (tw.api_response::jsonb->'retweeted_status')->>'full_text'

        when (tw.api_response::jsonb) ? 'quoted_status'
        then (
            tw.content ||
            (tw.api_response::jsonb->'quoted_status'->>'full_text')
        )

        else tw.content
    end, e'\t', ' ') as content
from tweet tw
    inner join user_tag ut using(user_id)
    inner join tag ta using(tag_id)
where
    ta.name = 'universe' and

    tw.create_dt >= '2021-01-01' and
    tw.create_dt <= '2021-03-01';

