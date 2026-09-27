{#
  silver.edits: one row per article edit event, forever (design section 7, data dictionary).
  Bronze dedups only within the 2-minute watermark; this MERGE on event_id removes the rest,
  including events that arrive again through a replay (bronze.edits_replay).
  Usernames never leave bronze: performer_user_text becomes a salted hash, raw_json is dropped.
#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    partitioned_by=['day(event_time)', 'wiki_id'],
    on_schema_change='fail',
) }}

{#- bronze.edits_replay exists only after the first replay (design section 8). -#}
{%- set replay = load_relation(source('bronze', 'edits_replay')) -%}

with live as (
    select *, 'live' as source
    from {{ source('bronze', 'edits') }}
    where namespace_id = 0
    {% if is_incremental() %}
      -- New bronze rows since the last run, with a 3-hour overlap for late commits.
      -- The overlap is harmless: MERGE on event_id never inserts a row twice.
      and ingested_at > (select max(ingested_at) - interval '3' hour from {{ this }})
    {% endif %}
),

{% if replay is not none %}
replayed as (
    -- Replayed events fill gaps. One the live job already delivered is left alone, so an
    -- edit keeps source = 'live' once it has it.
    select r.*, 'replay' as source
    from {{ replay }} as r
    where r.namespace_id = 0
    {% if is_incremental() %}
      and r.ingested_at > (select max(ingested_at) - interval '3' hour from {{ this }})
      and not exists (select 1 from {{ this }} as t where t.event_id = r.event_id)
    {% endif %}
),
{% endif %}

bronze as (
    select * from live
    {% if replay is not none %}
    union all
    select * from replayed
    {% endif %}
),

first_copy as (
    -- One copy per event in this run; the live copy wins over a replayed one.
    select *, row_number() over (
        partition by event_id
        order by case source when 'live' then 0 else 1 end, ingested_at
    ) as copy_number
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
