{%- macro trino__string_default_dtype(type=none) %}

{%- if type == 'rsrc' %}  
    {%- set global_var = var('datavault4dbt.rsrc_default_dtype', none) -%}
{%- elif type == 'stg' %}
    {%- set global_var = var('datavault4dbt.stg_default_dtype', none) -%}
{%- elif type == 'derived_columns' %}
    {%- set global_var = var('datavault4dbt.derived_columns_default_dtype', none) -%}
{%- else %}
    {%- set global_var = none %}
{%- endif %}

{%- set string_default_dtype = '' -%}

{%- if global_var is mapping -%}
    {%- if 'trino' in global_var.keys()|map('lower') -%}
        {% set string_default_dtype = global_var['trino'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt." ~ type ~ "_default_dtype' to a dictionary, but have not included the adapter you use (trino) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set string_default_dtype = "VARCHAR" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set string_default_dtype = global_var -%}
{%- else -%}
    {%- set string_default_dtype = "VARCHAR" -%}
{%- endif -%}

{{ return(string_default_dtype) }}

{%- endmacro -%}