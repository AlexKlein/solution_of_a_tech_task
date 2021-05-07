truncate datamart.top_bottom_five;
insert into datamart.top_bottom_five (label,
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
    top_five
    as (select sub_q.item,
               sub_q.rn,
               sub_q.year_month
        from  (select row_number() over (partition by year_month order by avg_rating desc) as rn,
                      year_month,
                      item
               from   base_q) sub_q
        where  sub_q.rn <= 5
        order by sub_q.rn),
    bottom_five
    as (select sub_q.item,
               sub_q.rn,
               sub_q.year_month
        from  (select row_number() over (partition by year_month order by avg_rating) as rn,
                      year_month,
                      item
               from   base_q) sub_q
        where  sub_q.rn <= 5
        order by sub_q.rn)
select concat('Top ', t.rn) as label,
       coalesce(
           nullif(m.title, ''),
           t.item) as movie,
       t.year_month
from   top_five t
left outer join core.metadata m
             on t.item = m.asin
union all
select concat('Bottom ', b.rn) as label,
       coalesce(
           nullif(m.title, ''),
           b.item) as movie,
       b.year_month
from   bottom_five b
left outer join core.metadata m
             on b.item = m.asin;
