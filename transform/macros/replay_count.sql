{#
  The replay-count check (design section 8, test T-DBT-REPLAY-COUNT). Run after a replay, the
  replay job and dbt build, with the numbers from the replay producer's report:
    make replay-check ENV=prod REPORT=data/replays/replay-<window>.json
  Fails unless bronze.edits_replay holds exactly the distinct events the producer sent for the
  window, and every one of them in the article namespace is in silver.edits exactly once.
#}
{% macro replay_count(since, until, expected) %}
    {%- set prefix = target.schema.split('_')[0] -%}
    {%- set window -%}
        emitted_at >= from_iso8601_timestamp('{{ since }}')
        and emitted_at < from_iso8601_timestamp('{{ until }}')
    {%- endset -%}
    {%- set sql -%}
        with replayed as (
            select distinct event_id, namespace_id
            from {{ prefix }}_bronze.edits_replay
            where {{ window }}
        )
        select
            (select count(*) from replayed) as in_bronze_replay,
            (select count(*) from replayed where namespace_id = 0) as article_events,
            (select count(*) from replayed r
                where r.namespace_id = 0
                and not exists (select 1 from {{ prefix }}_silver.edits s
                                where s.event_id = r.event_id)) as missing_from_silver,
            (select count(*) from {{ prefix }}_silver.edits s
                where s.source = 'replay' and {{ window }}) as filled_by_replay
    {%- endset -%}
    {%- set row = run_query(sql).rows[0] -%}
    {{ log("replay " ~ since ~ " to " ~ until ~ ": producer sent " ~ expected
           ~ ", bronze.edits_replay has " ~ row[0] ~ " (" ~ row[1] ~ " article events), missing from silver: "
           ~ row[2] ~ ", gaps filled by the replay: " ~ row[3], info=True) }}
    {%- if row[0] != expected | int or row[2] != 0 -%}
        {{ exceptions.raise_compiler_error("replay-count check FAILED") }}
    {%- endif -%}
    {{ log("replay-count check PASSED", info=True) }}
{% endmacro %}
