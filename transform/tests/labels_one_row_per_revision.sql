-- One label per (wiki_id, rev_id).
select wiki_id, rev_id, count(*) as n
from {{ ref('labels') }}
group by wiki_id, rev_id
having count(*) > 1
