select
    us.user_id,
    us.show_id,

    us.is_host,
    us.is_show,

    s.name as show_name
from twitter.user_show us
    inner join radio.show s using(show_id)
where
    not s.exclude;

