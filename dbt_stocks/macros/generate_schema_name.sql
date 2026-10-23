{#
  By default dbt names schemas "<target>_<custom>" (e.g. ANALYTICS_SILVER).
  We want clean layer names instead: BRONZE, SILVER, GOLD.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
