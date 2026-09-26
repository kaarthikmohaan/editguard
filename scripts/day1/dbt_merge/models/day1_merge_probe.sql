-- run=1 inserts e1..e3. run=2 changes e2 and adds e4, so MERGE must update 1 row and
-- insert 1. Running run=2 again must change nothing (idempotent MERGE on event_id).
{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key='event_id',
    partitioned_by=['day(event_time)'],
) }}

{% set run = var('run', 1) | int %}

select event_id, cast(event_time as timestamp(6)) as event_time, wiki_id, rev_size
from (
    values
        ('e1', timestamp '2026-09-25 10:00:00', 'enwiki', 100),
        ('e2', timestamp '2026-09-25 11:00:00', 'enwiki', {{ 200 if run == 1 else 201 }}),
        ('e3', timestamp '2026-09-26 09:00:00', 'hiwiki', 300)
        {% if run >= 2 %}, ('e4', timestamp '2026-09-26 10:00:00', 'bnwiki', 400){% endif %}
) as t (event_id, event_time, wiki_id, rev_size)
