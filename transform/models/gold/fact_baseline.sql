-- gold.fact_baseline: Wikimedia's score for each edit in fact_edit (design: a copy of
-- silver.baseline_scores for the edits in fact_edit). MERGE on (wiki_id, rev_id).
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['wiki_id', 'rev_id'],
    partitioned_by=['day(event_time)'],
    on_schema_change='fail',
) }}

select b.wiki_id, b.rev_id, b.event_time, b.model_name, b.model_version, b.probability_true,
    b.ingested_at
from {{ ref('baseline_scores') }} as b
inner join {{ ref('fact_edit') }} as f
    on f.wiki_id = b.wiki_id and f.rev_id = b.rev_id
{% if is_incremental() %}
where b.ingested_at > (select max(ingested_at) - interval '3' hour from {{ this }})
{% endif %}
