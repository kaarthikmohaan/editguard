{#
  silver.user_history_asof_hour: what was known about each editor at the end of each hour
  (design section 7 "Features", data dictionary). Rebuilt on every run.
    edits_30d             the editor's article edits in the 720 hours ending with asof_hour
    reverts_received_30d  of those hours, how many of the editor's edits were reverted by
                          someone else on the same page, counted when the revert happened
  Counted when it happened, not by final label: a label is only final 48 hours later, and
  using it here would let an hour "know" about reverts still in the future (leakage).
  Sparse: one row per hour in which a count changes (an edit, a revert received, or one of
  them leaving the 720-hour window). History for an edit at hour H is the latest row with
  asof_hour <= H - 1 hour; no row means no history (zero). The clock is labels_as_of().
#}
{# Rebuilt tables need a unique S3 location per build: dbt-athena swaps the new build in by rename. #}
{{ config(materialized='table', s3_data_naming='schema_table_unique') }}

{%- set as_of = labels_as_of() -%}

with edits as (
    select wiki_id, page_id, rev_id, event_time, user_hash
    from {{ ref('edits') }}
    where page_change_kind = 'edit' and user_hash is not null
),

reverted as (
    -- Each of the editor's edits reverted by someone else, at the time of its first revert.
    select e.wiki_id, e.user_hash, e.rev_id, min(r.event_time) as reverted_at
    from edits as e
    join {{ ref('edits') }} as r
      on r.wiki_id = e.wiki_id
     and r.page_id = e.page_id
     and r.revert_method is not null
     and e.rev_id between r.rev_reverted_oldest_id and r.rev_reverted_newest_id
     and r.event_time > e.event_time
     and r.user_hash <> e.user_hash
    group by e.wiki_id, e.user_hash, e.rev_id
),

changes as (
    -- +1 in the hour it happened, -1 when it leaves the 720-hour window.
    select wiki_id, user_hash, date_trunc('hour', event_time) as hour, 1 as d_edits, 0 as d_reverts
    from edits
    union all
    select wiki_id, user_hash, date_trunc('hour', event_time) + interval '720' hour, -1, 0
    from edits
    union all
    select wiki_id, user_hash, date_trunc('hour', reverted_at), 0, 1
    from reverted
    union all
    select wiki_id, user_hash, date_trunc('hour', reverted_at) + interval '720' hour, 0, -1
    from reverted
),

per_hour as (
    select wiki_id, user_hash, hour as asof_hour, sum(d_edits) as d_edits, sum(d_reverts) as d_reverts
    from changes
    where hour <= {{ as_of }}
    group by wiki_id, user_hash, hour
)

select
    wiki_id,
    user_hash,
    asof_hour,
    cast(sum(d_reverts) over (
        partition by wiki_id, user_hash order by asof_hour
        rows between unbounded preceding and current row
    ) as integer) as reverts_received_30d,
    cast(sum(d_edits) over (
        partition by wiki_id, user_hash order by asof_hour
        rows between unbounded preceding and current row
    ) as integer) as edits_30d
from per_hour
