{#
  Each editor's current state (design: dim_user_hashed holds current state only; attributes at
  edit time live on fact_edit). Rebuilt on every run from silver.edits.
#}
{# Rebuilt tables need a unique S3 location per build: dbt-athena swaps the new build in by rename. #}
{{ config(materialized='table', s3_data_naming='schema_table_unique') }}

with events as (
    select
        user_hash,
        event_time,
        performer_is_temp,
        performer_registration_dt,
        performer_groups,
        row_number() over (partition by user_hash order by event_time desc) as newest_first,
        min(event_time) over (partition by user_hash) as first_seen
    from {{ ref('edits') }}
    where user_hash is not null
)

select
    user_hash,
    performer_is_temp as is_temp,
    performer_registration_dt is not null and not performer_is_temp as is_registered,
    first_seen,
    performer_groups as current_groups
from events
where newest_first = 1
