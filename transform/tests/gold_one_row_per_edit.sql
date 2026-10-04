-- fact_edit, fact_baseline and fact_label have one row per (wiki_id, rev_id): their MERGE key.
select 'fact_edit' as model, wiki_id, rev_id, count(*) as copies
from {{ ref('fact_edit') }}
group by wiki_id, rev_id
having count(*) > 1
union all
select 'fact_baseline', wiki_id, rev_id, count(*)
from {{ ref('fact_baseline') }}
group by wiki_id, rev_id
having count(*) > 1
union all
select 'fact_label', wiki_id, rev_id, count(*)
from {{ ref('fact_label') }}
group by wiki_id, rev_id
having count(*) > 1
