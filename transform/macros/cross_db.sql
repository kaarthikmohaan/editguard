{#
  SQL that differs between Athena (Trino, prod and staging) and DuckDB (the ci target).
  default__ is Athena; duckdb__ is the ci version. Models call the plain names.
#}

{# YYYYMMDD integer of a date or timestamp, e.g. 20260927 (dim_date and fact_edit keys). #}
{% macro yyyymmdd(expr) -%}{{ return(adapter.dispatch('yyyymmdd')(expr)) }}{%- endmacro %}
{% macro default__yyyymmdd(expr) -%}cast(date_format({{ expr }}, '%Y%m%d') as integer){%- endmacro %}
{% macro duckdb__yyyymmdd(expr) -%}cast(strftime({{ expr }}, '%Y%m%d') as integer){%- endmacro %}

{# ISO day of week: 1 = Monday ... 7 = Sunday. #}
{% macro iso_day_of_week(expr) -%}{{ return(adapter.dispatch('iso_day_of_week')(expr)) }}{%- endmacro %}
{% macro default__iso_day_of_week(expr) -%}day_of_week({{ expr }}){%- endmacro %}
{% macro duckdb__iso_day_of_week(expr) -%}isodow({{ expr }}){%- endmacro %}

{# A FROM clause with one row per day in [start, end], column d (a date). #}
{% macro days_between(start, end) -%}{{ return(adapter.dispatch('days_between')(start, end)) }}{%- endmacro %}
{% macro default__days_between(start, end) -%}
    unnest(sequence(date '{{ start }}', date '{{ end }}', interval '1' day)) as t (d)
{%- endmacro %}
{% macro duckdb__days_between(start, end) -%}
    (select cast(unnest(generate_series(date '{{ start }}', date '{{ end }}', interval 1 day)) as date) as d) as t
{%- endmacro %}

{# The labels' clock: var('labels_as_of') (e.g. '2026-09-28 12:00:00', UTC) or now. #}
{% macro labels_as_of() -%}
    {%- if var('labels_as_of', none) -%}timestamp '{{ var("labels_as_of") }}'
    {%- else -%}{{ return(adapter.dispatch('now_utc')()) }}{%- endif -%}
{%- endmacro %}
{% macro default__now_utc() -%}cast(current_timestamp as timestamp(6)){%- endmacro %}
{% macro duckdb__now_utc() -%}current_timestamp{%- endmacro %}
