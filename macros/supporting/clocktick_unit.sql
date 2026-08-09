{%- macro clocktick_unit() %}

    {{ return( adapter.dispatch('clocktick_unit', 'datavault4dbt')() ) }}

{%- endmacro -%}


{%- macro default__clocktick_unit() %}

{%- set global_var = var('datavault4dbt.clocktick_unit', none) -%}
{%- set clocktick_unit = '' -%}

{%- if global_var is mapping -%}
    {%- if global_var.get('bigquery') is not none -%}
        {% set clocktick_unit = global_var.get('bigquery') %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.clocktick_unit' to a dictionary, but have not included the adapter you use (bigquery) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set clocktick_unit = "MICROSECOND" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set clocktick_unit = global_var -%}
{%- else -%}
    {%- set clocktick_unit = "MICROSECOND" -%}
{%- endif -%}

{{ return(clocktick_unit) }}

{%- endmacro -%}


{%- macro snowflake__clocktick_unit() %}

{%- set global_var = var('datavault4dbt.clocktick_unit', none) -%}
{%- set clocktick_unit = '' -%}

{%- if global_var is mapping -%}
    {%- if 'snowflake' in global_var.keys()|map('lower') -%}
        {% set clocktick_unit = global_var['snowflake'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.clocktick_unit' to a dictionary, but have not included the adapter you use (snowflake) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set clocktick_unit = "MICROSECOND" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set clocktick_unit = global_var -%}
{%- else -%}
    {%- set clocktick_unit = "MICROSECOND" -%}
{%- endif -%}

{{ return(clocktick_unit) }}

{%- endmacro -%}


{%- macro exasol__clocktick_unit() %}

{%- set global_var = var('datavault4dbt.clocktick_unit', none) -%}
{%- set clocktick_unit = '' -%}

{%- if global_var is mapping -%}
    {%- if 'exasol' in global_var.keys()|map('lower') -%}
        {% set clocktick_unit = global_var['exasol'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.clocktick_unit' to a dictionary, but have not included the adapter you use (exasol) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set clocktick_unit = "MILLISECOND" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set clocktick_unit = global_var -%}
{%- else -%}
    {%- set clocktick_unit = "MILLISECOND" -%}
{%- endif -%}

{{ return(clocktick_unit) }}

{%- endmacro -%}


{%- macro synapse__clocktick_unit() %}

{%- set global_var = var('datavault4dbt.clocktick_unit', none) -%}
{%- set clocktick_unit = '' -%}

{%- if global_var is mapping -%}    
    {%- if 'synapse' in global_var.keys()|map('lower') -%}
        {% set clocktick_unit = global_var['synapse'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.clocktick_unit' to a dictionary, but have not included the adapter you use (synapse) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set clocktick_unit = "NANOSECOND" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set clocktick_unit = global_var -%}
{%- else -%}        
    {%- set clocktick_unit = "NANOSECOND" -%}
{%- endif -%}

{{ return(clocktick_unit) }}

{%- endmacro -%}  


{%- macro postgres__clocktick_unit() %}

{%- set global_var = var('datavault4dbt.clocktick_unit', none) -%}
{%- set clocktick_unit = '' -%}

{%- if global_var is mapping -%}
    {%- if 'postgres' in global_var.keys()|map('lower') -%}
        {% set clocktick_unit = global_var['postgres'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.clocktick_unit' to a dictionary, but have not included the adapter you use (postgres) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set clocktick_unit = "MICROSECOND" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set clocktick_unit = global_var -%}
{%- else -%}
    {%- set clocktick_unit = "MICROSECOND" -%}
{%- endif -%}

{{ return(clocktick_unit) }}

{%- endmacro -%}


{%- macro redshift__clocktick_unit() %}

{%- set global_var = var('datavault4dbt.clocktick_unit', none) -%}
{%- set clocktick_unit = '' -%}

{%- if global_var is mapping -%}
    {%- if 'redshift' in global_var.keys()|map('lower') -%}
        {% set clocktick_unit = global_var['redshift'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.clocktick_unit' to a dictionary, but have not included the adapter you use (redshift) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set clocktick_unit = "MICROSECOND" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set clocktick_unit = global_var -%}
{%- else -%}
    {%- set clocktick_unit = "MICROSECOND" -%}
{%- endif -%}

{{ return(clocktick_unit) }}

{%- endmacro -%}


{%- macro fabric__clocktick_unit() %}

{%- set global_var = var('datavault4dbt.clocktick_unit', none) -%}
{%- set clocktick_unit = '' -%}

{%- if global_var is mapping -%}    
    {%- if 'fabric' in global_var.keys()|map('lower') -%}
        {% set clocktick_unit = global_var['fabric'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.clocktick_unit' to a dictionary, but have not included the adapter you use (fabric) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set clocktick_unit = "MICROSECOND" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set clocktick_unit = global_var -%}
{%- else -%}
    {%- set clocktick_unit = "MICROSECOND" -%}
{%- endif -%}

{{ return(clocktick_unit) }}

{%- endmacro -%}


{%- macro oracle__clocktick_unit() %}

{%- set global_var = var('datavault4dbt.clocktick_unit', none) -%}
{%- set clocktick_unit = '' -%}

{%- if global_var is mapping -%}
    {%- if 'oracle' in global_var.keys()|map('lower') -%}
        {% set clocktick_unit = global_var['oracle'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.clocktick_unit' to a dictionary, but have not included the adapter you use (Oracle) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set clocktick_unit = "MICROSECOND" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set clocktick_unit = global_var -%}
{%- else -%}
    {%- set clocktick_unit = "MICROSECOND" -%}
{%- endif -%}

{{ return(clocktick_unit) }}

{%- endmacro -%}


{%- macro databricks__clocktick_unit() %}

{%- set global_var = var('datavault4dbt.clocktick_unit', none) -%}
{%- set clocktick_unit = '' -%}

{%- if global_var is mapping -%}
    {%- if 'databricks' in global_var.keys()|map('lower') -%}
        {% set clocktick_unit = global_var['databricks'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.clocktick_unit' to a dictionary, but have not included the adapter you use (databricks) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set clocktick_unit = "MICROSECOND" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set clocktick_unit = global_var -%}
{%- else -%}
    {%- set clocktick_unit = "MICROSECOND" -%}
{%- endif -%}

{{ return(clocktick_unit) }}

{%- endmacro -%}


{%- macro trino__clocktick_unit() %}

{%- set global_var = var('datavault4dbt.clocktick_unit', none) -%}
{%- set clocktick_unit = '' -%}

{%- if global_var is mapping -%}
    {%- if 'trino' in global_var.keys()|map('lower') -%}
        {% set clocktick_unit = global_var['trino'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.clocktick_unit' to a dictionary, but have not included the adapter you use (trino) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set clocktick_unit = "MILLISECOND" -%}
    {%- endif -%}
{%- else -%}
    {%- if datavault4dbt.is_something(global_var) -%}
        {%- set clocktick_unit = global_var -%}
    {%- else -%}
        {%- set clocktick_unit = "MILLISECOND" -%}
    {%- endif -%}
{%- endif -%}

{{ return(clocktick_unit) }}

{%- endmacro -%}

{%- macro sqlserver__clocktick_unit() %}

{%- set global_var = var('datavault4dbt.clocktick_unit', none) -%}
{%- set clocktick_unit = '' -%}

{%- if global_var is mapping -%}
    {%- if 'sqlserver' in global_var.keys()|map('lower') -%}
        {% set clocktick_unit = global_var['sqlserver'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.clocktick_unit' to a dictionary, but have not included the adapter you use (sqlserver) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set clocktick_unit = "NANOSECOND" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set clocktick_unit = global_var -%}
{%- else -%}
    {%- set clocktick_unit = "NANOSECOND" -%}
{%- endif -%}

{{ return(clocktick_unit) }}

{%- endmacro -%}
