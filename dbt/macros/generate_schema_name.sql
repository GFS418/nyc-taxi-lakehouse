{#-
  Default dbt behaviour puts each folder in its own dataset: analytics_dev_staging, _marts, _seeds.
  That is right for dev and prod. In CI every run builds into one throwaway dataset instead, so the
  run has exactly one thing to create and one thing to drop.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.name == 'ci' or custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- else -%}
        {{ target.schema | trim }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
