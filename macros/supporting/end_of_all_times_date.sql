{%- macro end_of_all_times_date() %}

    {{ return(adapter.dispatch('end_of_all_times_date', 'datavault4dbt')()) }}

{%- endmacro -%}


{%- macro default__end_of_all_times_date() %}

{%- set global_var = var('datavault4dbt.end_of_all_times_date', none) -%}
{%- set end_of_all_times_date = '' -%}

{%- if global_var is mapping -%}
    {%- if 'bigquery' in global_var.keys()|map('lower') -%}
        {% set end_of_all_times_date = global_var['bigquery'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.end_of_all_times_date' to a dictionary, but have not included the adapter you use (bigquery) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set end_of_all_times_date = "8888-12-31" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set end_of_all_times_date = global_var -%}
{%- else -%}
    {%- set end_of_all_times_date = "8888-12-31" -%}
{%- endif -%}

{{ return(end_of_all_times_date) }}

{%- endmacro -%}


{%- macro snowflake__end_of_all_times_date() %}

{%- set global_var = var('datavault4dbt.end_of_all_times_date', none) -%}
{%- set end_of_all_times_date = '' -%}

{%- if global_var is mapping -%}
    {%- if 'snowflake' in global_var.keys()|map('lower') -%}
        {% set end_of_all_times_date = global_var['snowflake'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.end_of_all_times_date' to a dictionary, but have not included the adapter you use (snowflake) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set end_of_all_times_date = "8888-12-31" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set end_of_all_times_date = global_var -%}
{%- else -%}
    {%- set end_of_all_times_date = "8888-12-31" -%}
{%- endif -%}

{{ return(end_of_all_times_date) }}

{%- endmacro -%}


{%- macro exasol__end_of_all_times_date() %}

{%- set global_var = var('datavault4dbt.end_of_all_times_date', none) -%}
{%- set end_of_all_times_date = '' -%}

{%- if global_var is mapping -%}
    {%- if 'exasol' in global_var.keys()|map('lower') -%}
        {% set end_of_all_times_date = global_var['exasol'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.end_of_all_times_date' to a dictionary, but have not included the adapter you use (exasol) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set end_of_all_times_date = "8888-12-31" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set end_of_all_times_date = global_var -%}
{%- else -%}
    {%- set end_of_all_times_date = "8888-12-31" -%}
{%- endif -%}

{{ return(end_of_all_times_date) }}

{%- endmacro -%}


{%- macro postgres__end_of_all_times_date() %}

{%- set global_var = var('datavault4dbt.end_of_all_times_date', none) -%}
{%- set end_of_all_times_date = '' -%}

{%- if global_var is mapping -%}
    {%- if 'postgres' in global_var.keys()|map('lower') -%}
        {% set end_of_all_times_date = global_var['postgres'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.end_of_all_times_date' to a dictionary, but have not included the adapter you use (postgres) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set end_of_all_times_date = "8888-12-31" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set end_of_all_times_date = global_var -%}
{%- else -%}        
    {%- set end_of_all_times_date = "8888-12-31" -%}
{%- endif -%}

{{ return(end_of_all_times_date) }}    
{%- endmacro -%}


{%- macro redshift__end_of_all_times_date() %}

{%- set global_var = var('datavault4dbt.end_of_all_times_date', none) -%}
{%- set end_of_all_times_date = '' -%}

{%- if global_var is mapping -%}
    {%- if 'redshift' in global_var.keys()|map('lower') -%}
        {% set end_of_all_times_date = global_var['redshift'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.end_of_all_times_date' to a dictionary, but have not included the adapter you use (redshift) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set end_of_all_times_date = "8888-12-31" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set end_of_all_times_date = global_var -%}
{%- else -%}
    {%- set end_of_all_times_date = "8888-12-31" -%}
{%- endif -%}

{{ return(end_of_all_times_date) }}
{%- endmacro -%}


{%- macro synapse__end_of_all_times_date() %}

{%- set global_var = var('datavault4dbt.end_of_all_times_date', none) -%}
{%- set end_of_all_times_date = '' -%}

{%- if global_var is mapping -%}
    {%- if 'synapse' in global_var.keys()|map('lower') -%}
        {% set end_of_all_times_date = global_var['synapse'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.end_of_all_times_date' to a dictionary, but have not included the adapter you use (synapse) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set end_of_all_times_date = "8888-12-31" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set end_of_all_times_date = global_var -%}
{%- else -%}
    {%- set end_of_all_times_date = "8888-12-31" -%}
{%- endif -%}

{{ return(end_of_all_times_date) }}
{%- endmacro -%}


{%- macro fabric__end_of_all_times_date() %}

{%- set global_var = var('datavault4dbt.end_of_all_times_date', none) -%}
{%- set end_of_all_times_date = '' -%}

{%- if global_var is mapping -%}
    {%- if 'fabric' in global_var.keys()|map('lower') -%}
        {% set end_of_all_times_date = global_var['fabric'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.end_of_all_times_date' to a dictionary, but have not included the adapter you use (fabric) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set end_of_all_times_date = "8888-12-31" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set end_of_all_times_date = global_var -%}
{%- else -%}
    {%- set end_of_all_times_date = "8888-12-31" -%}
{%- endif -%}

{{ return(end_of_all_times_date) }}
{%- endmacro -%}