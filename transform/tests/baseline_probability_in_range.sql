-- A probability outside [0, 1] means the contract or the parser broke.
select wiki_id, rev_id, probability_true
from {{ ref('baseline_scores') }}
where probability_true < 0 or probability_true > 1
