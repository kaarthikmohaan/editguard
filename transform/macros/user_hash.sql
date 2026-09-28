{#
  Salted SHA-256 of a username (design: usernames hashed in silver and gold). The salt comes
  from Secrets Manager at run time (make dbt sets USERNAME_SALT) and is never committed.
  It does appear in compiled SQL under target/ (gitignored) and in Athena's query history.
  Both versions give the same 64 lowercase hex characters for the same salt and name.
#}
{% macro user_hash(column) -%}
    {{ return(adapter.dispatch('user_hash')(column)) }}
{%- endmacro %}

{% macro default__user_hash(column) -%}
    case when {{ column }} is not null
        then lower(to_hex(sha256(to_utf8('{{ env_var("USERNAME_SALT") }}' || ':' || {{ column }}))))
    end
{%- endmacro %}

{% macro duckdb__user_hash(column) -%}
    case when {{ column }} is not null
        then sha256('{{ env_var("USERNAME_SALT") }}' || ':' || {{ column }})
    end
{%- endmacro %}
