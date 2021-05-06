truncate core.metadata;
insert into core.metadata (asin,
                           title)
with
    clean_rows
    as (select replace(replace(replace(json_text, '"', ''), '\', ''), '""', '"') as clean_txt
        from   stage.metadata),
    unwrap_list
    as (select replace(substr(clean_txt, 2, length(clean_txt)-2), '''', '"') as unwrapped_txt
        from   clean_rows),
    title_base
    as (select substr(unwrapped_txt,
                      position('"asin": "' in unwrapped_txt) + 9,
                      10) as asin,
               substr(
                      substr(unwrapped_txt,
                             position('"title": "' in unwrapped_txt) + 10),
                      0,
                      position('": ' in
                               substr(unwrapped_txt,
                                      position('"title": "' in unwrapped_txt) + 10))) as title
        from   unwrap_list)
select distinct
       src.asin,
       substr(src.title, 0, length(src.title)-(length(src.title)-position('",  "' in src.title))) as title
from   title_base src
left outer join core.metadata tgt
             on src.asin = tgt.asin
where  tgt.asin is null;
