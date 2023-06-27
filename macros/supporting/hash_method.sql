{%- macro hash_method() %}

    {{ return( adapter.dispatch('hash_method', 'datavault4dbt')() ) }}

{%- endmacro -%}


{%- macro default__hash_method() %}

{%- set global_var = var('datavault4dbt.hash', none) -%}
{%- set hash_method = '' -%}

{%- if global_var is mapping -%}
    {%- if 'bigquery' in global_var.keys()|map('lower') -%}
        {% set hash_method = global_var['bigquery'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.hash' to a dictionary, but have not included the adapter you use (bigquery) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set hash_method = 'MD5' -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set hash_method = global_var -%}
{%- else -%}
    {%- set hash_method = 'MD5' -%}
{%- endif -%}

{{ return(hash_method) }}

{%- endmacro -%}


{%- macro snowflake__hash_method() %}

{%- set global_var = var('datavault4dbt.hash', none) -%}
{%- set hash_method = '' -%}

{%- if global_var is mapping -%}
    {%- if 'snowflake' in global_var.keys()|map('lower') -%}
        {% set hash_method = global_var['snowflake'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.hash' to a dictionary, but have not included the adapter you use (snowflake) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set hash_method = 'MD5' -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set hash_method = global_var -%}
{%- else -%}
    {%- set hash_method = 'MD5' -%}
{%- endif -%}

{{ return(hash_method) }}

{%- endmacro -%}


{%- macro exasol__hash_method() %}

{%- set global_var = var('datavault4dbt.hash', none) -%}
{%- set hash_method = '' -%}

{%- if global_var is mapping -%}
    {%- if 'exasol' in global_var.keys()|map('lower') -%}
        {% set hash_method = global_var['exasol'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.hash' to a dictionary, but have not included the adapter you use (exasol) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set hash_method = 'MD5' -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set hash_method = global_var -%}
{%- else -%}
    {%- set hash_method = 'MD5' -%}
{%- endif -%}

{{ return(hash_method) }}

{%- endmacro -%}
