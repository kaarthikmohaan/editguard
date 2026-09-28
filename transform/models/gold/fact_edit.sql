{#
  gold.fact_edit: one row per scored edit (non-bot article edits, the design's evaluated
  population), with the editor's attributes at edit time. MERGE on (wiki_id, rev_id).
  ingested_at is kept only to find new rows on incremental runs.
#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['wiki_id', 'rev_id'],
    partitioned_by=['day(event_time)'],
    on_schema_change='fail',
) }}

select
    wiki_id,
    rev_id,
    {{ yyyymmdd('event_time') }} as date_key,
    user_hash,
    event_time,
    page_id,
    byte_delta,
    is_minor_edit,
    performer_is_temp as is_temp_at_edit,
    performer_edit_count as edit_count_at_edit,
    cast(date_diff('day', performer_registration_dt, event_time) as integer)
        as account_age_days_at_edit,
    ingested_at
from {{ ref('edits') }}
where is_scored_population
{% if is_incremental() %}
  and ingested_at > (select max(ingested_at) - interval '3' hour from {{ this }})
{% endif %}
