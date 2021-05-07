truncate datamart.top_five_increased_rating;
insert into datamart.top_five_increased_rating (label,
                                                movie,
                                                year_month)
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
                      row_number() over (partition by year_month order by increase_amount desc) as top_rn
               from   agg_set
               where  rn <= 1) sub_q
        where  sub_q.top_rn <= 5
        order by sub_q.rn)
select concat('Top ', t.top_rn) as label,
       coalesce(
           nullif(m.title, ''),
           t.item) as movie,
       year_month
from   top_five t
left outer join core.metadata m
             on t.item = m.asin;
