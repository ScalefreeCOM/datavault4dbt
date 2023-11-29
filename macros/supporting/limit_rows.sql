{%- macro limit_rows() %}

{{- adapter.dispatch('limit_rows', 'datavault4dbt')() -}}

{%- endmacro -%}

{%- macro synapse__limit_rows() %}

{%- if target.schema == 'prod' %}
    {{ return('') }}
{%- else -%}
    {{ return('TOP 100') }}
{%- endif -%}

{%- endmacro -%}