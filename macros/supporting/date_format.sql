{%- macro date_format() %}

    {{ return(adapter.dispatch('date_format', 'datavault4dbt')()) }}

{%- endmacro -%}


{%- macro default__date_format() %}

{%- set global_var = var('datavault4dbt.date_format', none) -%}
{%- set date_format = '' -%}

{%- if global_var is mapping -%}
    {%- if 'bigquery' in global_var.keys()|map('lower') -%}
        {% set date_format = global_var['bigquery'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.date_format' to a dictionary, but have not included the adapter you use (bigquery) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set date_format = "%Y-%m-%d" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set date_format = global_var -%}
{%- else -%}
    {%- set date_format = "%Y-%m-%d" -%}
{%- endif -%}

{{ return(date_format) }}

{%- endmacro -%}


{%- macro snowflake__date_format() %}

{%- set global_var = var('datavault4dbt.date_format', none) -%}
{%- set date_format = '' -%}

{%- if global_var is mapping -%}
    {%- if 'snowflake' in global_var.keys()|map('lower') -%}
        {% set date_format = global_var['snowflake'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.date_format' to a dictionary, but have not included the adapter you use (snowflake) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set date_format = "YYYY-MM-DD" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set date_format = global_var -%}
{%- else -%}
    {%- set date_format = "YYYY-MM-DD" -%}
{%- endif -%}

{{ return(date_format) }}

{%- endmacro -%}


{%- macro exasol__date_format() %}

{%- set global_var = var('datavault4dbt.date_format', none) -%}
{%- set date_format = '' -%}

{%- if global_var is mapping -%}
    {%- if 'exasol' in global_var.keys()|map('lower') -%}
        {% set date_format = global_var['exasol'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.date_format' to a dictionary, but have not included the adapter you use (exasol) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set date_format = "YYYY-mm-dd" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set date_format = global_var -%}
{%- else -%}
    {%- set date_format = "YYYY-mm-dd" -%}
{%- endif -%}

{{ return(date_format) }}

{%- endmacro -%}


{%- macro synapse__date_format() %}

{%- set global_var = var('datavault4dbt.date_format', none) -%}
{%- set date_format = '' -%}

{%- if global_var is mapping -%}
    {%- if 'synapse' in global_var.keys()|map('lower') -%}
        {% set date_format = global_var['synapse'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.date_format' to a dictionary, but have not included the adapter you use (synapse) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set date_format = "yyyy-MM-dd" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set date_format = global_var -%}
{%- else -%}  
    {%- set date_format = "yyyy-MM-dd" -%}
{%- endif -%}

{{ return(date_format) }} 

{%- endmacro -%}


{%- macro postgres__date_format() %}

{%- set global_var = var('datavault4dbt.date_format', none) -%}
{%- set date_format = '' -%}

{%- if global_var is mapping -%}
    {%- if 'postgres' in global_var.keys()|map('lower') -%}
        {% set date_format = global_var['postgres'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.date_format' to a dictionary, but have not included the adapter you use (postgres) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set date_format = "YYYY-MM-DD" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set date_format = global_var -%}
{%- else -%}
    {%- set date_format = "YYYY-MM-DD" -%}
{%- endif -%}

{{ return(date_format) }}

{%- endmacro -%}

{%- macro redshift__date_format() %}

{%- set global_var = var('datavault4dbt.date_format', none) -%}
{%- set date_format = '' -%}

{%- if global_var is mapping -%}
    {%- if 'redshift' in global_var.keys()|map('lower') -%}
        {% set date_format = global_var['redshift'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.date_format' to a dictionary, but have not included the adapter you use (redshift) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set date_format = "YYYY-MM-DD" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set date_format = global_var -%}
{%- else -%}
    {%- set date_format = "YYYY-MM-DD" -%}
{%- endif -%}

{{ return(date_format) }}

{%- endmacro -%}


{%- macro fabric__date_format() %}

{%- set global_var = var('datavault4dbt.date_format', none) -%}
{%- set date_format = '' -%}

{%- if global_var is mapping -%}
    {%- if 'fabric' in global_var.keys()|map('lower') -%}
        {% set date_format = global_var['fabric'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.date_format' to a dictionary, but have not included the adapter you use (fabric) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set date_format = "yyyy-mm-dd" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set date_format = global_var -%}
{%- else -%}  
    {%- set date_format = "yyyy-mm-dd" -%}
{%- endif -%}

{{ return(date_format) }} 

{%- endmacro -%}


{%- macro databricks__date_format() %}

{%- set global_var = var('datavault4dbt.date_format', none) -%}
{%- set date_format = '' -%}

{%- if global_var is mapping -%}
    {%- if 'databricks' in global_var.keys()|map('lower') -%}
        {% set date_format = global_var['databricks'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.date_format' to a dictionary, but have not included the adapter you use (databricks) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set date_format = "yyyy-mm-dd" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set date_format = global_var -%}
{%- else -%}  
    {%- set date_format = "yyyy-mm-dd" -%}
{%- endif -%}

{{ return(date_format) }} 

{%- endmacro -%}


{%- macro oracle__date_format() %}

{%- set global_var = var('datavault4dbt.date_format', none) -%}
{%- set date_format = '' -%}

{%- if global_var is mapping -%}
    {%- if 'oracle' in global_var.keys()|map('lower') -%}
        {% set date_format = global_var['oracle'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.date_format' to a dictionary, but have not included the adapter you use (oracle) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set date_format = "YYYY-MM-DD" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set date_format = global_var -%}
{%- else -%}
    {%- set date_format = "YYYY-MM-DD" -%}
{%- endif -%}

{{ return(date_format) }}

{%- endmacro -%}


{%- macro sqlserver__date_format() %}

{%- set global_var = var('datavault4dbt.date_format', none) -%}
{%- set date_format = '' -%}

{%- if global_var is mapping -%}
    {%- if 'sqlserver' in global_var.keys()|map('lower') -%}
        {% set date_format = global_var['sqlserver'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.date_format' to a dictionary, but have not included the adapter you use (sqlserver) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set date_format = "yyyy-mm-dd" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set date_format = global_var -%}
{%- else -%}  
    {%- set date_format = "yyyy-mm-dd" -%}
{%- endif -%}

{{ return(date_format) }} 

{%- endmacro -%}