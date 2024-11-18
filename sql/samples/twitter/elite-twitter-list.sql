select
    u.user_id,
    u.screen_name
from twitter.user u
    inner join twitter.user_tag ut using(user_id)
where
    ut.tag = 'universe';

