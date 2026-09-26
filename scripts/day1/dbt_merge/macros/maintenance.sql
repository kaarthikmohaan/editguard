{% macro optimize_probe() %}
  {% do run_query("OPTIMIZE stg_silver.day1_merge_probe REWRITE DATA USING BIN_PACK") %}
  {{ log("OPTIMIZE succeeded", info=True) }}
{% endmacro %}

{% macro vacuum_probe() %}
  {% do run_query("VACUUM stg_silver.day1_merge_probe") %}
  {{ log("VACUUM succeeded", info=True) }}
{% endmacro %}

{% macro drop_probe() %}
  {% do run_query("DROP TABLE IF EXISTS stg_silver.day1_merge_probe") %}
  {{ log("dropped stg_silver.day1_merge_probe", info=True) }}
{% endmacro %}
