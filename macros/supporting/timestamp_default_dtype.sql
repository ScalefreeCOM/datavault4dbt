{%- macro timestamp_default_dtype() %}

    {{ return( adapter.dispatch('timestamp_default_dtype', 'datavault4dbt')() ) }}

{%- endmacro -%}


{%- macro default__timestamp_default_dtype() %}

{%- set global_var = var('datavault4dbt.timestamp_default_dtype', none) -%}
{%- set timestamp_default_dtype = '' -%}

{%- if global_var is mapping -%}
    {%- if 'bigquery' in global_var.keys()|map('lower') -%}
        {% set timestamp_default_dtype = global_var['bigquery'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.timestamp_default_dtype' to a dictionary, but have not included the adapter you use (bigquery) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set timestamp_default_dtype = "TIMESTAMP" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set timestamp_default_dtype = global_var -%}
{%- else -%}
    {%- set timestamp_default_dtype = "TIMESTAMP" -%}
{%- endif -%}

{{ return(timestamp_default_dtype) }}

{%- endmacro -%}


{%- macro snowflake__timestamp_default_dtype() %}

{%- set global_var = var('datavault4dbt.timestamp_default_dtype', none) -%}
{%- set timestamp_default_dtype = '' -%}

{%- if global_var is mapping -%}
    {%- if 'snowflake' in global_var.keys()|map('lower') -%}
        {% set timestamp_default_dtype = global_var['snowflake'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.timestamp_default_dtype' to a dictionary, but have not included the adapter you use (snowflake) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set timestamp_default_dtype = "TIMESTAMP_TZ" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set timestamp_default_dtype = global_var -%}
{%- else -%}
    {%- set timestamp_default_dtype = "TIMESTAMP_TZ" -%}
{%- endif -%}

{{ return(timestamp_default_dtype) }}

{%- endmacro -%}


{%- macro exasol__timestamp_default_dtype() %}

{%- set global_var = var('datavault4dbt.timestamp_default_dtype', none) -%}
{%- set timestamp_default_dtype = '' -%}

{%- if global_var is mapping -%}
    {%- if 'exasol' in global_var.keys()|map('lower') -%}
        {% set timestamp_default_dtype = global_var['exasol'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.timestamp_default_dtype' to a dictionary, but have not included the adapter you use (exasol) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set timestamp_default_dtype = "TIMESTAMP(3) WITH LOCAL TIME ZONE" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set timestamp_default_dtype = global_var -%}
{%- else -%}
    {%- set timestamp_default_dtype = "TIMESTAMP(3) WITH LOCAL TIME ZONE" -%}
{%- endif -%}

{{ return(timestamp_default_dtype) }}

{%- endmacro -%}


{%- macro synapse__timestamp_default_dtype() %}

{%- set global_var = var('datavault4dbt.timestamp_default_dtype', none) -%}
{%- set timestamp_default_dtype = '' -%}

{%- if global_var is mapping -%}    
    {%- if 'synapse' in global_var.keys()|map('lower') -%}
        {% set timestamp_default_dtype = global_var['synapse'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.timestamp_default_dtype' to a dictionary, but have not included the adapter you use (synapse) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set timestamp_default_dtype = "datetimeoffset" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set timestamp_default_dtype = global_var -%}
{%- else -%}        
    {%- set timestamp_default_dtype = "datetimeoffset" -%}
{%- endif -%}

{{ return(timestamp_default_dtype) }}

{%- endmacro -%}  


{%- macro postgres__timestamp_default_dtype() %}

{%- set global_var = var('datavault4dbt.timestamp_default_dtype', none) -%}
{%- set timestamp_default_dtype = '' -%}

{%- if global_var is mapping -%}
    {%- if 'postgres' in global_var.keys()|map('lower') -%}
        {% set timestamp_default_dtype = global_var['postgres'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.timestamp_default_dtype' to a dictionary, but have not included the adapter you use (postgres) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set timestamp_default_dtype = "TIMESTAMPTZ" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set timestamp_default_dtype = global_var -%}
{%- else -%}
    {%- set timestamp_default_dtype = "TIMESTAMPTZ" -%}
{%- endif -%}

{{ return(timestamp_default_dtype) }}

{%- endmacro -%}


{%- macro redshift__timestamp_default_dtype() %}

{%- set global_var = var('datavault4dbt.timestamp_default_dtype', none) -%}
{%- set timestamp_default_dtype = '' -%}

{%- if global_var is mapping -%}
    {%- if 'redshift' in global_var.keys()|map('lower') -%}
        {% set timestamp_default_dtype = global_var['redshift'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.timestamp_default_dtype' to a dictionary, but have not included the adapter you use (redshift) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set timestamp_default_dtype = "TIMESTAMPTZ" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set timestamp_default_dtype = global_var -%}
{%- else -%}
    {%- set timestamp_default_dtype = "TIMESTAMPTZ" -%}
{%- endif -%}

{{ return(timestamp_default_dtype) }}

{%- endmacro -%}

{%- macro oracle__timestamp_default_dtype() %}

{%- set global_var = var('datavault4dbt.timestamp_default_dtype', none) -%}
{%- set timestamp_default_dtype = '' -%}

{%- if global_var is mapping -%}
    {%- if 'oracle' in global_var.keys()|map('lower') -%}
        {% set timestamp_default_dtype = global_var['oracle'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.timestamp_default_dtype' to a dictionary, but have not included the adapter you use (redshift) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set timestamp_default_dtype = "TIMESTAMP WITH TIME ZONE" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set timestamp_default_dtype = global_var -%}
{%- else -%}
    {%- set timestamp_default_dtype = "TIMESTAMP WITH TIME ZONE" -%}
{%- endif -%}

{{ return(timestamp_default_dtype) }}

{%- endmacro -%}
