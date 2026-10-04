-- gold.fact_label: the final label of each edit in fact_edit (design: a frozen copy of
-- silver.labels for the edits in fact_edit). Only final labels: an edit appears here once it
-- is 48 hours old, and its label never changes after that. MERGE on (wiki_id, rev_id).
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['wiki_id', 'rev_id'],
    partitioned_by=['day(event_time)'],
    on_schema_change='fail',
) }}

select l.wiki_id, l.rev_id, l.event_time, l.label, l.reverting_rev_id, l.revert_method,
    l.reverter_is_bot, l.minutes_to_revert, l.label_final_at
from {{ ref('labels') }} as l
inner join {{ ref('fact_edit') }} as f
    on f.wiki_id = l.wiki_id and f.rev_id = l.rev_id
where l.label <> 'label_unknown'
{% if is_incremental() %}
  and not exists (
      select 1 from {{ this }} as t where t.wiki_id = l.wiki_id and t.rev_id = l.rev_id
  )
{% endif %}
