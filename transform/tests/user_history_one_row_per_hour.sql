-- One row per (wiki_id, user_hash, asof_hour), and counts never negative.
select wiki_id, user_hash, asof_hour, count(*) as n, min(edits_30d) as e, min(reverts_received_30d) as r
from {{ ref('user_history_asof_hour') }}
group by wiki_id, user_hash, asof_hour
having count(*) > 1 or min(edits_30d) < 0 or min(reverts_received_30d) < 0
