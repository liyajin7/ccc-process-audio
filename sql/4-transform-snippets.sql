/*
 * Snippet fact data
 */

begin;
    set constraints all deferred;

    insert into radio.snippet
        (station_id, show_id, audio_file_offset, audio_file_index, content,
         approxhash, audio_key, raw_callsign, raw_show_name, show_confidence,
         diary_speaker_id, diary_band, diary_env, mean_word_confidence,
         diary_gender, show_source, date, wordcount, duration, start_dt, end_dt)
    select
        --
        -- FKs
        --

        scm.station_id as station_id,

        -- at most one level of hierarchy, so no need for a recursive CTE
        coalesce(sh.parent_show_id, sh.show_id) as show_id,

        --
        -- properly snippet-level fields
        --

        ss.segment_start_relative as audio_file_offset,
        ss.segment_idx as audio_file_index,

        coalesce(ss.content, '') as content,
        case
            when ss.content is null then null
            else ss.approxhash
        end as approxhash,

        ss.audio_key,
        ss.callsign as raw_callsign,
        ss.show_name as raw_show_name,
        ss.show_confidence,
        ss.diary_speaker_id,
        ss.diary_band,
        ss.diary_env,
        ss.mean_word_confidence,
        ss.diary_gender,
        ss.show_source,

        -- precompute some things for convenience
        date(to_timestamp(ss.segment_start_global)) as date,
        coalesce(array_length(regexp_split_to_array(ss.content, '\s+'), 1), 0) as wordcount,
        ss.segment_end_global - ss.segment_start_global as duration,
        to_timestamp(ss.segment_start_global) as start_dt,
        to_timestamp(ss.segment_end_global) as end_dt
    from stg.snippet ss
        left join stg.callsign_map scm on scm.corpus_callsign = ss.callsign
        left join radio.show sh on trim(lower(sh.name)) = trim(lower(ss.show_name));

    set constraints all immediate;

    -- recover space if loading all at once, or prep for next batch if
    -- loading incrementally
    truncate table stg.snippet;
commit;

