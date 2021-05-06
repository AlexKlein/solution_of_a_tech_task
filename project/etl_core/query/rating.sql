truncate core.rating;
insert into core.rating (user_id,
                         item,
                         rating,
                         event_timestamp)
select distinct
       src.user_id,
       src.item,
       src.rating,
       timestamp  'epoch' + src.event_timestamp * interval '1 second' as event_timestamp
from   stage.rating src
left outer join stage.rating tgt
             on src.user_id = tgt.user_id and
                src.item = tgt.item and
                src.event_timestamp = tgt.event_timestamp;
