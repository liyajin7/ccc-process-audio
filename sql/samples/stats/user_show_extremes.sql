with showstats as
(
    select
        sn.show_id,
        sum(sn.duration) as num_seconds,
        sum(sn.wordcount) as num_words
    from radio.snippet_data sn
    group by 1
)
select
    ss.show_id,

    max(sh.name) as show_name,
    max(ss.num_seconds) as num_seconds,
    max(ss.num_words) as num_words,

    string_agg(u.screen_name, ',') as twitter_handles
from showstats ss
    inner join radio.show sh using(show_id)
    inner join twitter.user_show us using(show_id)
    inner join twitter.user u using(user_id)
group by 1
order by 3 desc;

