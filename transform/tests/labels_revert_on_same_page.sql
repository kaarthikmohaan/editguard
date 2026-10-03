-- T-DBT-LABEL-PAGE (ADR 0005): a label's reverting edit is on the same page as the edit.
-- Returns the offending labels; the test passes when it returns none.
select l.wiki_id, l.rev_id, l.reverting_rev_id
from {{ ref('labels') }} as l
join {{ ref('edits') }} as e on e.wiki_id = l.wiki_id and e.rev_id = l.rev_id
join {{ ref('edits') }} as r on r.wiki_id = l.wiki_id and r.rev_id = l.reverting_rev_id
where l.reverting_rev_id is not null and r.page_id <> e.page_id
