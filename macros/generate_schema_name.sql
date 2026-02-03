{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Override dbt's default schema naming.

        Default behaviour appends custom_schema to the target
        schema (e.g. analytics_staging). This override uses the
        custom_schema directly when provided, so +schema: staging
        produces a dataset called 'staging', not 'analytics_staging'.

        When no custom_schema is set, falls back to the target
        schema from profiles.yml (analytics).
    #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
