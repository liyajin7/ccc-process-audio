select
    sn.snippet_id,
    sn.audio_key,
    sn.audio_file_offset,
    sn.audio_file_index,
    sn.start_dt,
    sn.end_dt,
    sn.duration
from radio.snippet sn
    inner join datasets.paper_round_3_snippets;
