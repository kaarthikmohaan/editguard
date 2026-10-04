{#
  silver.labels: one label per article edit (design section 7 "Labels", ADR 0005).
    damaging       a later revert on the same (wiki_id, page_id) covers this rev_id, by someone
                   other than the author, and that revert was not itself reverted
    bot_caught     the same, but by a bot account within 5 minutes
    ok             not reverted within 48 hours
    label_unknown  younger than 48 hours: not final yet. (Edits from before recording started
                   are not in silver.edits, so they get no row at all.)
  Only reverts within the edit's first 48 hours count, so a label never changes after
  label_final_at: an incremental run skips edits whose label is already final (frozen).
  The clock is var('labels_as_of') when set (tests, reports), otherwise now.
#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['wiki_id', 'rev_id'],
    partitioned_by=['day(event_time)'],
    on_schema_change='fail',
) }}

{%- set as_of = labels_as_of() -%}

with edits as (
    select e.wiki_id, e.page_id, e.rev_id, e.event_time, e.user_hash
    from {{ ref('edits') }} as e
    where e.page_change_kind = 'edit'
    {% if is_incremental() %}
      -- Frozen labels never change: skip edits that already have a final one.
      and not exists (
          select 1 from {{ this }} as t
          where t.wiki_id = e.wiki_id and t.rev_id = e.rev_id and t.label <> 'label_unknown'
      )
    {% endif %}
),

reverts as (
    select
        wiki_id, page_id, rev_id, event_time, user_hash, performer_is_bot,
        revert_method, rev_reverted_oldest_id, rev_reverted_newest_id
    from {{ ref('edits') }}
    where revert_method is not null and rev_reverted_oldest_id is not null
    {% if is_incremental() %}
      and event_time >= (select min(event_time) from edits)
    {% endif %}
),

covering as (
    -- Reverts on the same page (revision IDs are unique per wiki, not per page: ADR 0005),
    -- by someone else, within the edit's first 48 hours, whose range includes the edit.
    select
        e.wiki_id, e.page_id, e.rev_id, e.event_time,
        r.rev_id as reverting_rev_id,
        r.event_time as reverted_at,
        r.revert_method,
        r.performer_is_bot as reverter_is_bot
    from edits as e
    join reverts as r
      on r.wiki_id = e.wiki_id
     and r.page_id = e.page_id
     and e.rev_id between r.rev_reverted_oldest_id and r.rev_reverted_newest_id
     and r.event_time > e.event_time
     and r.event_time <= e.event_time + interval '48' hour
     and r.user_hash <> e.user_hash
),

effective as (
    -- A revert that was itself reverted in the same 48 hours restored the edit (T-U-EDGE-05).
    select c.*
    from covering as c
    where not exists (
        select 1 from reverts as u
        where u.wiki_id = c.wiki_id
          and u.page_id = c.page_id
          and c.reverting_rev_id between u.rev_reverted_oldest_id and u.rev_reverted_newest_id
          and u.event_time > c.reverted_at
          and u.event_time <= c.event_time + interval '48' hour
    )
),

first_revert as (
    select *, row_number() over (
        partition by wiki_id, rev_id order by reverted_at, reverting_rev_id
    ) as revert_number
    from effective
)

select
    e.wiki_id,
    e.rev_id,
    e.event_time,
    case
        when e.event_time + interval '48' hour > {{ as_of }} then 'label_unknown'
        when f.reverting_rev_id is null then 'ok'
        when f.reverter_is_bot and f.reverted_at <= e.event_time + interval '5' minute then 'bot_caught'
        else 'damaging'
    end as label,
    f.reverting_rev_id,
    f.revert_method,
    f.reverter_is_bot,
    date_diff('second', e.event_time, f.reverted_at) / 60.0 as minutes_to_revert,
    e.event_time + interval '48' hour as label_final_at
from edits as e
left join first_revert as f
  on f.wiki_id = e.wiki_id and f.rev_id = e.rev_id and f.revert_number = 1
