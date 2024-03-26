{%- macro beginning_of_all_times() %}

    {{ return( adapter.dispatch('beginning_of_all_times', 'datavault4dbt')() ) }}

{%- endmacro -%}


{%- macro default__beginning_of_all_times() %}

{%- set global_var = var('datavault4dbt.beginning_of_all_times', none) -%}
{%- set beginning_of_all_times = '' -%}

{%- if global_var is mapping -%}
    {%- if 'bigquery' in global_var.keys()|map('lower') -%}
        {% set beginning_of_all_times = global_var['bigquery'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.beginning_of_all_times' to a dictionary, but have not included the adapter you use (bigquery) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set beginning_of_all_times = "0001-01-01T00-00-01" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set beginning_of_all_times = global_var -%}
{%- else -%}
    {%- set beginning_of_all_times = "0001-01-01T00-00-01" -%}
{%- endif -%}

{{ return(beginning_of_all_times) }}

{%- endmacro -%}


{%- macro snowflake__beginning_of_all_times() %}

{%- set global_var = var('datavault4dbt.beginning_of_all_times', none) -%}
{%- set beginning_of_all_times = '' -%}

{%- if global_var is mapping -%}
    {%- if 'snowflake' in global_var.keys()|map('lower') -%}
        {% set beginning_of_all_times = global_var['snowflake'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.beginning_of_all_times' to a dictionary, but have not included the adapter you use (snowflake) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set beginning_of_all_times = "0001-01-01T00:00:01" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set beginning_of_all_times = global_var -%}
{%- else -%}
    {%- set beginning_of_all_times = "0001-01-01T00:00:01" -%}
{%- endif -%}

{{ return(beginning_of_all_times) }}

{%- endmacro -%}


{%- macro exasol__beginning_of_all_times() %}

{%- set global_var = var('datavault4dbt.beginning_of_all_times', none) -%}
{%- set beginning_of_all_times = '' -%}

{%- if global_var is mapping -%}
    {%- if 'exasol' in global_var.keys()|map('lower') -%}
        {% set beginning_of_all_times = global_var['exasol'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.beginning_of_all_times' to a dictionary, but have not included the adapter you use (exasol) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set beginning_of_all_times = "0001-01-01 00:00:01" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set beginning_of_all_times = global_var -%}
{%- else -%}
    {%- set beginning_of_all_times = "0001-01-01 00:00:01" -%}
{%- endif -%}

{{ return(beginning_of_all_times) }}

{%- endmacro -%}


{%- macro synapse__beginning_of_all_times() %}

{%- set global_var = var('datavault4dbt.beginning_of_all_times', none) -%}
{%- set beginning_of_all_times = '' -%}

{%- if global_var is mapping -%}    
    {%- if 'synapse' in global_var.keys()|map('lower') -%}
        {% set beginning_of_all_times = global_var['synapse'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.beginning_of_all_times' to a dictionary, but have not included the adapter you use (synapse) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set beginning_of_all_times = "1901-01-01T00:00:01" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set beginning_of_all_times = global_var -%}
{%- else -%}        
    {%- set beginning_of_all_times = "1901-01-01T00:00:01" -%}
{%- endif -%}

{{ return(beginning_of_all_times) }}

{%- endmacro -%}  


{%- macro postgres__beginning_of_all_times() %}

{%- set global_var = var('datavault4dbt.beginning_of_all_times', none) -%}
{%- set beginning_of_all_times = '' -%}

{%- if global_var is mapping -%}
    {%- if 'postgres' in global_var.keys()|map('lower') -%}
        {% set beginning_of_all_times = global_var['postgres'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.beginning_of_all_times' to a dictionary, but have not included the adapter you use (postgres) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set beginning_of_all_times = "0001-01-01 00:00:01" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set beginning_of_all_times = global_var -%}
{%- else -%}
    {%- set beginning_of_all_times = "0001-01-01 00:00:01" -%}
{%- endif -%}

{{ return(beginning_of_all_times) }}

{%- endmacro -%}


{%- macro redshift__beginning_of_all_times() %}

{%- set global_var = var('datavault4dbt.beginning_of_all_times', none) -%}
{%- set beginning_of_all_times = '' -%}

{%- if global_var is mapping -%}
    {%- if 'redshift' in global_var.keys()|map('lower') -%}
        {% set beginning_of_all_times = global_var['redshift'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.beginning_of_all_times' to a dictionary, but have not included the adapter you use (redshift) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set beginning_of_all_times = "0001-01-01 00:00:01" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set beginning_of_all_times = global_var -%}
{%- else -%}
    {%- set beginning_of_all_times = "0001-01-01 00:00:01" -%}
{%- endif -%}

{{ return(beginning_of_all_times) }}

{%- endmacro -%}