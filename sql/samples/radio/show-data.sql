select
    sn.show_id,

    max(sn.show_name) as show_name,
    bool_and(sn.syndicated_show) as syndicated_show,

    count(*) as nsnippets,
    sum(sn.duration) as duration,

    sum(sn.is_public::int) as nsnippets_public,

    sum(sn.is_public::int) / count(*)::float as public_fraction
from radio.snippet_data sn
where
    sn.show_id is not null
group by 1;

