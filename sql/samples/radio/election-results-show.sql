select
    show_id,

    votes_pres_dem,
    votes_pres_rep,
    votes_pres_total,

    votes_pres_dem / votes_pres_total as d_share,
    votes_pres_dem / (votes_pres_dem + votes_pres_rep) as d_share_2way
from datasets.votes_by_show;

