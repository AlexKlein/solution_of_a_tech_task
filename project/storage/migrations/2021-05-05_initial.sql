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
create table if not exists datamart.top_bottom_five (label      varchar(16),
                                                     movie      varchar(512),
                                                     year_month varchar(8));
create table if not exists datamart.top_five_increased_rating (label      varchar(16),
                                                               movie      varchar(512),
                                                               year_month varchar(8));
