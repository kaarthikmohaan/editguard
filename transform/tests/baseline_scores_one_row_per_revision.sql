-- silver.baseline_scores has one row per (wiki_id, rev_id): the MERGE key.
select wiki_id, rev_id, count(*) as copies
from {{ ref('baseline_scores') }}
group by wiki_id, rev_id
having count(*) > 1
