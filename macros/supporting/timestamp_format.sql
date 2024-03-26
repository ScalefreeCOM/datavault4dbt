{%- macro timestamp_format() %}

    {{ return(adapter.dispatch('timestamp_format', 'datavault4dbt')()) }}

{%- endmacro -%}


{%- macro default__timestamp_format() %}

{%- set global_var = var('datavault4dbt.timestamp_format', none) -%}
{%- set timestamp_format = '' -%}

{%- if global_var is mapping -%}
    {%- if 'bigquery' in global_var.keys()|map('lower') -%}
        {% set timestamp_format = global_var['bigquery'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.timestamp_format' to a dictionary, but have not included the adapter you use (bigquery) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set timestamp_format = "%Y-%m-%dT%H-%M-%S" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set timestamp_format = global_var -%}
{%- else -%}
    {%- set timestamp_format = "%Y-%m-%dT%H-%M-%S" -%}
{%- endif -%}

{{ return(timestamp_format) }}

{%- endmacro -%}


{%- macro snowflake__timestamp_format() %}

{%- set global_var = var('datavault4dbt.timestamp_format', none) -%}
{%- set timestamp_format = '' -%}

{%- if global_var is mapping -%}
    {%- if 'snowflake' in global_var.keys()|map('lower') -%}
        {% set timestamp_format = global_var['snowflake'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.timestamp_format' to a dictionary, but have not included the adapter you use (snowflake) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set timestamp_format = "YYYY-MM-DDTHH24:MI:SS" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set timestamp_format = global_var -%}
{%- else -%}
    {%- set timestamp_format = "YYYY-MM-DDTHH24:MI:SS" -%}
{%- endif -%}

{{ return(timestamp_format) }}

{%- endmacro -%}


{%- macro exasol__timestamp_format() %}

{%- set global_var = var('datavault4dbt.timestamp_format', none) -%}
{%- set timestamp_format = '' -%}

{%- if global_var is mapping -%}
    {%- if 'exasol' in global_var.keys()|map('lower') -%}
        {% set timestamp_format = global_var['exasol'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.timestamp_format' to a dictionary, but have not included the adapter you use (exasol) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set timestamp_format = "YYYY-mm-dd HH:MI:SS" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set timestamp_format = global_var -%}
{%- else -%}
    {%- set timestamp_format = "YYYY-mm-dd HH:MI:SS" -%}
{%- endif -%}

{{ return(timestamp_format) }}

{%- endmacro -%}


{%- macro synapse__timestamp_format() %}

{%- set global_var = var('datavault4dbt.timestamp_format', none) -%}
{%- set timestamp_format = '' -%}

{%- if global_var is mapping -%}
    {%- if 'synapse' in global_var.keys()|map('lower') -%}
        {% set timestamp_format = global_var['synapse'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.timestamp_format' to a dictionary, but have not included the adapter you use (synapse) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set timestamp_format = 127 -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set timestamp_format = global_var -%}
{%- else -%}  
    {%- set timestamp_format = 126 -%}
{%- endif -%}

{{ return(timestamp_format) }} 

{%- endmacro -%}


{%- macro postgres__timestamp_format() %}

{%- set global_var = var('datavault4dbt.timestamp_format', none) -%}
{%- set timestamp_format = '' -%}

{%- if global_var is mapping -%}
    {%- if 'postgres' in global_var.keys()|map('lower') -%}
        {% set timestamp_format = global_var['postgres'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.timestamp_format' to a dictionary, but have not included the adapter you use (postgres) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set timestamp_format = "%Y-%m-%dT%H-%M-%S" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set timestamp_format = global_var -%}
{%- else -%}
    {%- set timestamp_format = "%Y-%m-%dT%H-%M-%S" -%}
{%- endif -%}

{{ return(timestamp_format) }}

{%- endmacro -%}

{%- macro redshift__timestamp_format() %}

{%- set global_var = var('datavault4dbt.timestamp_format', none) -%}
{%- set timestamp_format = '' -%}

{%- if global_var is mapping -%}
    {%- if 'redshift' in global_var.keys()|map('lower') -%}
        {% set timestamp_format = global_var['redshift'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.timestamp_format' to a dictionary, but have not included the adapter you use (redshift) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set timestamp_format = "YYYY-MM-DD HH24:MI:SS" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set timestamp_format = global_var -%}
{%- else -%}
    {%- set timestamp_format = "YYYY-MM-DD HH24:MI:SS" -%}
{%- endif -%}

{{ return(timestamp_format) }}

{%- endmacro -%}

