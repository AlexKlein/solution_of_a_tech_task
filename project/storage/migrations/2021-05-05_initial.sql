create schema if not exists stage;
create table if not exists stage.metadata (json_text  text,
                                           created_at date);
create table if not exists stage.rating (user_id          varchar(128),
                                         item             varchar(32),
                                         rating           numeric,
                                         event_timestamp  numeric,
                                         created_at       date);
create schema if not exists core;
create table if not exists core.metadata (asin  varchar(32),
                                          title varchar(512));
create table if not exists core.rating (user_id          varchar(128),
                                        item             varchar(32),
                                        rating           numeric,
                                        event_timestamp  timestamp) partition by range (event_timestamp);
create table if not exists core.rating_y1900 partition of core.rating
    for values from ('1900-01-01') TO ('2000-01-01');
create table if not exists core.rating_y2000 partition of core.rating
    for values from ('2000-01-01') TO ('2001-01-01');
create table if not exists core.rating_y2001 partition of core.rating
    for values from ('2001-01-01') TO ('2002-01-01');
create table if not exists core.rating_y2002 partition of core.rating
    for values from ('2002-01-01') TO ('2003-01-01');
create table if not exists core.rating_y2003 partition of core.rating
    for values from ('2003-01-01') TO ('2004-01-01');
create table if not exists core.rating_y2004 partition of core.rating
    for values from ('2004-01-01') TO ('2005-01-01');
create table if not exists core.rating_y2005 partition of core.rating
    for values from ('2005-01-01') TO ('2006-01-01');
create table if not exists core.rating_y2006 partition of core.rating
    for values from ('2006-01-01') TO ('2007-01-01');
create table if not exists core.rating_y2007 partition of core.rating
    for values from ('2007-01-01') TO ('2008-01-01');
create table if not exists core.rating_y2008 partition of core.rating
    for values from ('2008-01-01') TO ('2009-01-01');
create table if not exists core.rating_y2009 partition of core.rating
    for values from ('2009-01-01') TO ('2010-01-01');
create table if not exists core.rating_y2010 partition of core.rating
    for values from ('2010-01-01') TO ('2011-01-01');
create table if not exists core.rating_y2011 partition of core.rating
    for values from ('2011-01-01') TO ('2012-01-01');
create table if not exists core.rating_y2012 partition of core.rating
    for values from ('2012-01-01') TO ('2013-01-01');
create table if not exists core.rating_y2013 partition of core.rating
    for values from ('2013-01-01') TO ('2014-01-01');
create table if not exists core.rating_y2014 partition of core.rating
    for values from ('2014-01-01') TO ('2015-01-01');
create table if not exists core.rating_y2015 partition of core.rating
    for values from ('2015-01-01') TO ('2016-01-01');
create table if not exists core.rating_y2016 partition of core.rating
    for values from ('2016-01-01') TO ('2017-01-01');
create table if not exists core.rating_y2017 partition of core.rating
    for values from ('2017-01-01') TO ('2018-01-01');
create table if not exists core.rating_y2018 partition of core.rating
    for values from ('2018-01-01') TO ('2019-01-01');
create table if not exists core.rating_y2019 partition of core.rating
    for values from ('2019-01-01') TO ('2020-01-01');
create table if not exists core.rating_y2020 partition of core.rating
    for values from ('2020-01-01') TO ('2021-01-01');
create table if not exists core.rating_y2021 partition of core.rating
    for values from ('2021-01-01') TO ('2022-01-01');
create table if not exists core.rating_y6000 partition of core.rating
    for values from ('2022-01-01') TO ('6000-01-01');
create schema if not exists datamart;
create or replace view datamart.v_top_bottom_five
as
    with
        base_q
        as (select avg(rating) as avg_rating,
                   concat(
                       extract(year from event_timestamp),
                       '.',
                       extract(month from event_timestamp)) as year_month,
                   item
            from   core.rating
            group by concat(
                         extract(year from event_timestamp),
                         '.',
                         extract(month from event_timestamp)),
                     item
            order by year_month),
        top_five
        as (select sub_q.item,
                   sub_q.rn
            from  (select row_number() over (partition by year_month, item order by avg_rating desc) as rn,
                          year_month,
                          item
                   from   base_q) sub_q
            where  sub_q.rn <= 5
            order by sub_q.rn),
        bottom_five
        as (select sub_q.item,
                   sub_q.rn
            from  (select row_number() over (partition by year_month, item order by avg_rating) as rn,
                          year_month,
                          item
                   from   base_q) sub_q
            where  sub_q.rn <= 5
            order by sub_q.rn)
    select concat('Top ', t.rn) as label,
           coalesce(
               nullif(m.title, ''),
               t.item) as movie
    from   top_five t
    left outer join core.metadata m
                 on t.item = m.asin
    union all
    select concat('Bottom ', b.rn) as label,
           coalesce(
               nullif(m.title, ''),
               b.item) as movie
    from   bottom_five b
    left outer join core.metadata m
                 on b.item = m.asin;
create or replace view datamart.v_top_five_increased_rating
as
    with
        base_q
        as (select avg(rating) as avg_rating,
                   concat(
                       extract(year from event_timestamp),
                       '.',
                       extract(month from event_timestamp)) as year_month,
                   item
            from   core.rating
            group by concat(
                         extract(year from event_timestamp),
                         '.',
                         extract(month from event_timestamp)),
                     item
            order by year_month),
        lag_added
        as (select year_month,
                   item,
                   avg_rating,
                   lag(avg_rating) over (partition by item order by year_month) as lag_avg_rating
            from   base_q),
        clean_set
        as (select year_month,
                   item,
                   avg_rating,
                   lag_avg_rating,
                   avg_rating - lag_avg_rating as increase_amount
            from   lag_added
            where  lag_avg_rating is not null and
                   lag_avg_rating < avg_rating),
        agg_set
        as (select year_month,
                   item,
                   avg_rating,
                   lag_avg_rating,
                   increase_amount,
                   row_number() over (partition by item order by increase_amount desc, year_month asc) as rn
            from   clean_set),
        top_five
        as (select sub_q.item,
                   sub_q.year_month,
                   sub_q.top_rn
            from  (select year_month,
                          item,
                          avg_rating,
                          lag_avg_rating,
                          increase_amount,
                          rn,
                          row_number() over (order by increase_amount desc) as top_rn
                   from   agg_set
                   where  rn <= 1) sub_q
            where  sub_q.top_rn <= 5
            order by sub_q.rn)
    select concat('Top ', t.top_rn) as label,
           year_month,
           coalesce(
               nullif(m.title, ''),
               t.item) as movie
    from   top_five t
    left outer join core.metadata m
                 on t.item = m.asin;
