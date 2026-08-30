{# Use custom schema names as written rather than prefixing the target schema.
   Correct for a single-developer local project; a shared warehouse wants the
   default prefixing so developers don't collide. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
