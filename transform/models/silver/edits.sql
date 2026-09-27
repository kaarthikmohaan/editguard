{#
  silver.edits: one row per article edit event, forever (design section 7, data dictionary).
  Bronze dedups only within the 2-minute watermark; this MERGE on event_id removes the rest.
  Usernames never leave bronze: performer_user_text becomes a salted hash, raw_json is dropped.
#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    partitioned_by=['day(event_time)', 'wiki_id'],
    on_schema_change='fail',
) }}

with bronze as (
    select *, 'live' as source
    from {{ source('bronze', 'edits') }}
    where namespace_id = 0
    {% if is_incremental() %}
      -- New bronze rows since the last run, with a 3-hour overlap for late commits.
      -- The overlap is harmless: MERGE on event_id never inserts a row twice.
      and ingested_at > (select max(ingested_at) - interval '3' hour from {{ this }})
    {% endif %}
),

first_copy as (
    select *, row_number() over (partition by event_id order by ingested_at) as copy_number
    from bronze
)

select
    event_id,
    event_time,
    emitted_at,
    ingested_at,
    wiki_id,
    page_id,
    page_title,
    namespace_id,
    page_change_kind,
    rev_id,
    rev_parent_id,
    rev_size,
    prior_rev_size,
    rev_size - prior_rev_size as byte_delta,
    is_minor_edit,
    comment,
    is_content_visible,
    is_comment_visible,
    is_editor_visible,
    {{ user_hash('performer_user_text') }} as user_hash,
    performer_is_bot,
    performer_is_temp,
    performer_groups,
    performer_edit_count,
    performer_registration_dt,
    revert_method,
    rev_reverted_oldest_id,
    rev_reverted_newest_id,
    rev_original_id,
    upstream_topic,
    upstream_partition,
    upstream_offset,
    schema_version,
    source,
    (page_change_kind = 'edit' and not performer_is_bot) as is_scored_population
from first_copy
where copy_number = 1
