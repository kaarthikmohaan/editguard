{#
  silver.baseline_scores: Wikimedia's revert-risk score for each article edit, one row per
  (wiki_id, rev_id) (design section 7; ADR 0011). event_time comes from the edit in silver.edits,
  so scores for edits outside silver (other namespaces) are left out. If a revision was scored
  more than once, the newest score wins.
#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['wiki_id', 'rev_id'],
    partitioned_by=['day(event_time)'],
    on_schema_change='fail',
) }}

with scores as (
    select *
    from {{ source('bronze', 'baseline_scores') }}
    {% if is_incremental() %}
      -- New scores since the last run, with a 3-hour overlap. A score whose edit was not yet
      -- in silver.edits on the previous run is picked up again here; MERGE keeps one row.
      where ingested_at > (select max(ingested_at) - interval '3' hour from {{ this }})
    {% endif %}
),

-- The revision itself: its edit or create event (later delete or visibility events repeat rev_id).
revisions as (
    select wiki_id, rev_id, event_time
    from {{ ref('edits') }}
    where page_change_kind in ('edit', 'create')
),

joined as (
    select
        s.wiki_id,
        s.rev_id,
        r.event_time,
        s.model_name,
        s.model_version,
        s.probability_true,
        s.ingested_at,
        row_number() over (
            partition by s.wiki_id, s.rev_id order by s.ingested_at desc
        ) as newest_first
    from scores as s
    inner join revisions as r
        on r.wiki_id = s.wiki_id and r.rev_id = s.rev_id
)

select wiki_id, rev_id, event_time, model_name, model_version, probability_true, ingested_at
from joined
where newest_first = 1
