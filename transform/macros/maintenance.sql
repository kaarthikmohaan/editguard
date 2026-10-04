{#
  Iceberg table maintenance on Athena (design: dbt-athena OPTIMIZE and VACUUM; ADR 0009 showed
  OPTIMIZE is safe while Spark appends). Run daily:
    make dbt ENV=prod CMD="run-operation maintain_tables"
  Whole tables (first run, or after a replay into old days):
    make dbt ENV=prod CMD="run-operation maintain_tables --args '{days: null}'"

  OPTIMIZE rewrites small data files into larger ones (bronze gets one small file per wiki per
  minute). Athena bills OPTIMIZE for every byte in the partitions it touches, even ones already
  compacted, so the daily run covers only today and yesterday (by event_time).
  VACUUM expires snapshots older than the retention and deletes files that no snapshot refers
  to once they are older than the retention too (this includes the metadata.json files left
  from before the 2026-09-27 metadata cleanup fix).
#}

{# Snapshot retention: 7 days (runbook: Iceberg tables, snapshots 7 days). #}
{% macro snapshot_retention_seconds() %}{{ return(604800) }}{% endmacro %}

{# Each incremental table with the timestamp column its daily OPTIMIZE filters on. Rebuilt
   tables (the gold dimensions) are fresh on every run and need no maintenance. #}
{% macro maintained_tables() %}
    {%- set prefix = target.schema.split('_')[0] -%}
    {{ return([
        [prefix ~ '_bronze.edits', 'event_time'],
        [prefix ~ '_bronze.baseline_scores', 'ingested_at'],
        [prefix ~ '_bronze.edits_replay', 'event_time'],
        [prefix ~ '_silver.edits', 'event_time'],
        [prefix ~ '_silver.baseline_scores', 'event_time'],
        [prefix ~ '_silver.labels', 'event_time'],
        [prefix ~ '_gold.fact_edit', 'event_time'],
        [prefix ~ '_gold.fact_baseline', 'event_time'],
        [prefix ~ '_gold.fact_label', 'event_time'],
    ]) }}
{% endmacro %}

{% macro set_retention(table, seconds) %}
    {% do run_query(
        "ALTER TABLE " ~ table ~ " SET TBLPROPERTIES ("
        ~ "'vacuum_max_snapshot_age_seconds' = '" ~ seconds ~ "', "
        ~ "'vacuum_max_metadata_files_to_keep' = '100')"
    ) %}
{% endmacro %}

{% macro optimize_sql(table, days, time_column='event_time') %}
    {%- set sql = "OPTIMIZE " ~ table ~ " REWRITE DATA USING BIN_PACK" -%}
    {%- if days is not none -%}
        {%- set sql = sql ~ " WHERE " ~ time_column ~ " >= current_date - interval '" ~ days ~ "' day" -%}
    {%- endif -%}
    {{ return(sql) }}
{% endmacro %}

{% macro maintain_table(table, days=1, retention_seconds=none, time_column='event_time') %}
    {%- set retention = retention_seconds or snapshot_retention_seconds() -%}
    {% do set_retention(table, retention) %}
    {% do run_query(optimize_sql(table, days, time_column)) %}
    {{ log("OPTIMIZE " ~ table ~ " (" ~ ("all days" if days is none else "last " ~ days ~ " day(s) + today") ~ "): done", info=True) }}
    {% do run_query("VACUUM " ~ table) %}
    {{ log("VACUUM " ~ table ~ ": done (retention " ~ retention ~ " s)", info=True) }}
{% endmacro %}

{% macro maintain_tables(days=1) %}
    {% for table, time_column in maintained_tables() %}
        {%- set db, name = table.split('.') -%}
        {%- if adapter.get_relation(database=target.database, schema=db, identifier=name) is none -%}
            {{ log("skip " ~ table ~ ": not created yet", info=True) }}
            {%- continue -%}
        {%- endif -%}
        {% do maintain_table(table, days, time_column=time_column) %}
    {% endfor %}
{% endmacro %}
