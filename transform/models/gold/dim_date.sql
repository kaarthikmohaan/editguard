-- One row per UTC day from the first recording (2026-09-25) to the end of 2027.
{# Rebuilt tables need a unique S3 location per build: dbt-athena swaps the new build in by rename. #}
{{ config(materialized='table', s3_data_naming='schema_table_unique') }}

select
    cast(date_format(d, '%Y%m%d') as integer) as date_key,
    d as date,
    day_of_week(d) as day_of_week,
    day_of_week(d) in (6, 7) as is_weekend
from unnest(sequence(date '2026-09-25', date '2027-12-31', interval '1' day)) as t (d)
