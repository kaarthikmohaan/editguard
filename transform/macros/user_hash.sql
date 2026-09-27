{#
  Salted SHA-256 of a username (design: usernames hashed in silver and gold). The salt comes
  from Secrets Manager at run time (make dbt sets USERNAME_SALT) and is never committed.
  It does appear in compiled SQL under target/ (gitignored) and in Athena's query history.
#}
{% macro user_hash(column) -%}
    case when {{ column }} is not null
        then lower(to_hex(sha256(to_utf8('{{ env_var("USERNAME_SALT") }}' || ':' || {{ column }}))))
    end
{%- endmacro %}
