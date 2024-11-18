/*
 * Populate the twitter.user_show table
 */

begin;
    set constraints all deferred;

    drop table if exists tmp_show_twitter;
    create local temporary table tmp_show_twitter
    on commit preserve rows as
    select distinct
        sst.show_id,
        unnest(string_to_array(replace(sst.twitter, ' ', ''), ',')) as screen_name
    from stg.show_twitter sst;

    drop table if exists tmp_show_host;
    create local temporary table tmp_show_host
    on commit preserve rows as
    select distinct
        sst.show_id,
        unnest(string_to_array(replace(sst.host, ' ', ''), ',')) as screen_name
    from stg.show_twitter sst;

    drop table if exists tmp_show_show;
    create local temporary table tmp_show_show
    on commit preserve rows as
    select distinct
        sst.show_id,
        unnest(string_to_array(replace(sst.show, ' ', ''), ',')) as screen_name
    from stg.show_twitter sst;

    insert into twitter.user_show
        (user_id, show_id, is_host, is_show)
    select
        tu.user_id,
        tst.show_id,

        tsh.screen_name is not null,

        tss.screen_name is not null
    from tmp_show_twitter tst
        inner join twitter.user tu on
            lower(tu.screen_name) = lower(tst.screen_name)

        left join tmp_show_host tsh on
            tsh.show_id = tst.show_id and
            lower(tsh.screen_name) = lower(tst.screen_name)

        left join tmp_show_show tss on
            tss.show_id = tst.show_id and
            lower(tss.screen_name) = lower(tst.screen_name);

    set constraints all immediate;

    analyze twitter.user;
    analyze twitter.user_show;
commit;

