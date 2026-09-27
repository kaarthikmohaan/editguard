{#
  Glue databases are <prefix>_<layer> (stg_silver, prod_gold). dbt's default would build
  "<target schema>_<layer>" (prod_silver_gold), so map the layer onto the environment prefix.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set prefix = target.schema.split('_')[0] -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ prefix }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
