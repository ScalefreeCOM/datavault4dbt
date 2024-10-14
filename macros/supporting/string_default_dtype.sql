{%- macro string_default_dtype(type=none) %}

    {{ return( adapter.dispatch('string_default_dtype', 'datavault4dbt')(type=type) ) }}

{%- endmacro -%}


{%- macro default__string_default_dtype(type) %}

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
    {%- if 'bigquery' in global_var.keys()|map('lower') -%}
        {% set string_default_dtype = global_var['bigquery'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt." ~ type ~ "_default_dtype' to a dictionary, but have not included the adapter you use (bigquery) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set string_default_dtype = "STRING" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set string_default_dtype = global_var -%}
{%- else -%}
    {%- set string_default_dtype = "STRING" -%}
{%- endif -%}

{{ return(string_default_dtype) }}

{%- endmacro -%}


{%- macro snowflake__string_default_dtype(type) %}

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
    {%- if 'snowflake' in global_var.keys()|map('lower') -%}
        {% set string_default_dtype = global_var['snowflake'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt." ~ type ~ "_default_dtype' to a dictionary, but have not included the adapter you use (snowflake) as a key. Applying the default value.") -%}
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


{%- macro exasol__string_default_dtype(type) %}

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
    {%- if 'exasol' in global_var.keys()|map('lower') -%}
        {% set string_default_dtype = global_var['exasol'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt." ~ type ~ "_default_dtype' to a dictionary, but have not included the adapter you use (exasol) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set string_default_dtype = "VARCHAR (2000000) UTF8" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set string_default_dtype = global_var -%}
{%- else -%}
    {%- set string_default_dtype = "VARCHAR (2000000) UTF8" -%}
{%- endif -%}

{{ return(string_default_dtype) }}

{%- endmacro -%}


{%- macro synapse__string_default_dtype(type) %}

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
    {%- if 'synapse' in global_var.keys()|map('lower') -%}
        {% set string_default_dtype = global_var['synapse'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt." ~ type ~ "_default_dtype' to a dictionary, but have not included the adapter you use (synapse) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set string_default_dtype = "NVARCHAR(1000)" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set string_default_dtype = global_var -%}
{%- else -%}        
    {%- set string_default_dtype = "NVARCHAR(1000)" -%}
{%- endif -%}

{{ return(string_default_dtype) }}

{%- endmacro -%}  


{%- macro postgres__string_default_dtype(type) %}

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
    {%- if 'postgres' in global_var.keys()|map('lower') -%}
        {% set string_default_dtype = global_var['postgres'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt." ~ type ~ "_default_dtype' to a dictionary, but have not included the adapter you use (postgres) as a key. Applying the default value.") -%}
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


{%- macro redshift__string_default_dtype(type) %}

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
    {%- if 'redshift' in global_var.keys()|map('lower') -%}
        {% set string_default_dtype = global_var['redshift'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt." ~ type ~ "_default_dtype' to a dictionary, but have not included the adapter you use (redshift) as a key. Applying the default value.") -%}
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

{%- macro oracle__string_default_dtype(type) %}

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
    {%- if 'oracle' in global_var.keys()|map('lower') -%}
        {% set string_default_dtype = global_var['oracle'] %}
    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt." ~ type ~ "_default_dtype' to a dictionary, but have not included the adapter you use (Oracle) as a key. Applying the default value.") -%}
        {% endif %}
        {%- set string_default_dtype = "VARCHAR2(40)" -%}
    {% endif %}
{%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
    {%- set string_default_dtype = global_var -%}
{%- else -%}
    {%- set string_default_dtype = "VARCHAR2(40)" -%}
{%- endif -%}

{{ return(string_default_dtype) }}

{%- endmacro -%}
