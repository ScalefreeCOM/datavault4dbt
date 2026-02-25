{%- macro first_day_of_week() %}
    {{ return(adapter.dispatch('first_day_of_week', 'datavault4dbt')()) }}
{%- endmacro -%}

{%- macro default__first_day_of_week() %}

    {%- set global_var = var('datavault4dbt.first_day_of_week', none) -%}
    {%- set first_day = 1 -%}

    {%- if global_var is mapping -%}
        {%- if target.type in global_var.keys()|map('lower') -%}
            {%- set first_day = global_var[target.type] -%}
        {%- else -%}
            {%- if execute -%}
                {%- do exceptions.warn("Warning: Adapter not found in 'datavault4dbt.first_day_of_week' dictionary. Applying default (1).") -%}
            {%- endif -%}
        {%- endif -%}

    {%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
        {%- set first_day = global_var -%}
    {%- endif -%}

    {{ return(first_day | int) }}

{%- endmacro -%}

{%- macro bigquery__first_day_of_week() %}

    {%- set global_var = var('datavault4dbt.first_day_of_week', none) -%}
    {%- set first_day = 1 -%}

    {%- if global_var is mapping -%}
        {%- if 'bigquery' in global_var.keys()|map('lower') -%}
            {%- set first_day = global_var['bigquery'] -%}
        {%- else -%}
            {%- if execute -%}
                {%- do exceptions.warn("Warning: Adapter not found in 'datavault4dbt.first_day_of_week' dictionary. Applying default (1).") -%}
            {%- endif -%}
        {%- endif -%}

    {%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
        {%- set first_day = global_var -%}
    {%- endif -%}

    {{ return(first_day | int) }}

{%- endmacro -%}

{%- macro snowflake__first_day_of_week() %}

    {%- set global_var = var('datavault4dbt.first_day_of_week', none) -%}
    {%- set first_day = 1 -%}

    {%- if global_var is mapping -%}
        {%- if 'snowflake' in global_var.keys()|map('lower') -%}
            {%- set first_day = global_var['snowflake'] -%}
        {%- else -%}
            {%- if execute -%}
                {%- do exceptions.warn("Warning: Adapter not found in 'datavault4dbt.first_day_of_week' dictionary. Applying default (1).") -%}
            {%- endif -%}
        {%- endif -%}

    {%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
        {%- set first_day = global_var -%}
    {%- endif -%}

    {{ return(first_day | int) }}

{%- endmacro -%}

{%- macro exasol__first_day_of_week() %}

    {%- set global_var = var('datavault4dbt.first_day_of_week', none) -%}
    {%- set first_day = 1 -%}

    {%- if global_var is mapping -%}
        {%- if 'exasol' in global_var.keys()|map('lower') -%}
            {%- set first_day = global_var['exasol'] -%}
        {%- else -%}
            {%- if execute -%}
                {%- do exceptions.warn("Warning: Adapter not found in 'datavault4dbt.first_day_of_week' dictionary. Applying default (1).") -%}
            {%- endif -%}
        {%- endif -%}

    {%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
        {%- set first_day = global_var -%}
    {%- endif -%}

    {{ return(first_day | int) }}

{%- endmacro -%}

{%- macro postgres__first_day_of_week() %}

    {%- set global_var = var('datavault4dbt.first_day_of_week', none) -%}
    {%- set first_day = 1 -%}

    {%- if global_var is mapping -%}
        {%- if 'postgres' in global_var.keys()|map('lower') -%}
            {%- set first_day = global_var['postgres'] -%}
        {%- else -%}
            {%- if execute -%}
                {%- do exceptions.warn("Warning: Adapter not found in 'datavault4dbt.first_day_of_week' dictionary. Applying default (1).") -%}
            {%- endif -%}
        {%- endif -%}

    {%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
        {%- set first_day = global_var -%}
    {%- endif -%}

    {{ return(first_day | int) }}

{%- endmacro -%}

{%- macro redshift__first_day_of_week() %}

    {%- set global_var = var('datavault4dbt.first_day_of_week', none) -%}
    {%- set first_day = 1 -%}

    {%- if global_var is mapping -%}
        {%- if 'redshift' in global_var.keys()|map('lower') -%}
            {%- set first_day = global_var['redshift'] -%}
        {%- else -%}
            {%- if execute -%}
                {%- do exceptions.warn("Warning: Adapter not found in 'datavault4dbt.first_day_of_week' dictionary. Applying default (1).") -%}
            {%- endif -%}
        {%- endif -%}

    {%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
        {%- set first_day = global_var -%}
    {%- endif -%}

    {{ return(first_day | int) }}

{%- endmacro -%}

{%- macro synapse__first_day_of_week() %}

    {%- set global_var = var('datavault4dbt.first_day_of_week', none) -%}
    {%- set first_day = 1 -%}

    {%- if global_var is mapping -%}
        {%- if 'synapse' in global_var.keys()|map('lower') -%}
            {%- set first_day = global_var['synapse'] -%}
        {%- else -%}
            {%- if execute -%}
                {%- do exceptions.warn("Warning: Adapter not found in 'datavault4dbt.first_day_of_week' dictionary. Applying default (1).") -%}
            {%- endif -%}
        {%- endif -%}

    {%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
        {%- set first_day = global_var -%}
    {%- endif -%}

    {{ return(first_day | int) }}

{%- endmacro -%}

{%- macro fabric__first_day_of_week() %}

    {%- set global_var = var('datavault4dbt.first_day_of_week', none) -%}
    {%- set first_day = 1 -%}

    {%- if global_var is mapping -%}
        {%- if 'fabric' in global_var.keys()|map('lower') -%}
            {%- set first_day = global_var['fabric'] -%}
        {%- else -%}
            {%- if execute -%}
                {%- do exceptions.warn("Warning: Adapter not found in 'datavault4dbt.first_day_of_week' dictionary. Applying default (1).") -%}
            {%- endif -%}
        {%- endif -%}

    {%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
        {%- set first_day = global_var -%}
    {%- endif -%}

    {{ return(first_day | int) }}

{%- endmacro -%}

{%- macro oracle__first_day_of_week() %}

    {%- set global_var = var('datavault4dbt.first_day_of_week', none) -%}
    {%- set first_day = 1 -%}

    {%- if global_var is mapping -%}
        {%- if 'oracle' in global_var.keys()|map('lower') -%}
            {%- set first_day = global_var['oracle'] -%}
        {%- else -%}
            {%- if execute -%}
                {%- do exceptions.warn("Warning: Adapter not found in 'datavault4dbt.first_day_of_week' dictionary. Applying default (1).") -%}
            {%- endif -%}
        {%- endif -%}

    {%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
        {%- set first_day = global_var -%}
    {%- endif -%}

    {{ return(first_day | int) }}

{%- endmacro -%}

{%- macro sqlserver__first_day_of_week() %}

    {%- set global_var = var('datavault4dbt.first_day_of_week', none) -%}
    {%- set first_day = 1 -%}

    {%- if global_var is mapping -%}
        {%- if 'sqlserver' in global_var.keys()|map('lower') -%}
            {%- set first_day = global_var['sqlserver'] -%}
        {%- else -%}
            {%- if execute -%}
                {%- do exceptions.warn("Warning: Adapter not found in 'datavault4dbt.first_day_of_week' dictionary. Applying default (1).") -%}
            {%- endif -%}
        {%- endif -%}

    {%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
        {%- set first_day = global_var -%}
    {%- endif -%}

    {{ return(first_day | int) }}

{%- endmacro -%}

{%- macro databricks__first_day_of_week() %}

    {%- set global_var = var('datavault4dbt.first_day_of_week', none) -%}
    {%- set first_day = 1 -%}

    {%- if global_var is mapping -%}
        {%- if 'databricks' in global_var.keys()|map('lower') -%}
            {%- set first_day = global_var['databricks'] -%}
        {%- else -%}
            {%- if execute -%}
                {%- do exceptions.warn("Warning: Adapter not found in 'datavault4dbt.first_day_of_week' dictionary. Applying default (7).") -%}
            {%- endif -%}
        {%- endif -%}

    {%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}
        {%- set first_day = global_var -%}
    {%- endif -%}

    {{ return(first_day | int) }}

{%- endmacro -%}