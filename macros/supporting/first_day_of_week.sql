{%- macro first_day_of_week() %}
    {{ return(adapter.dispatch('first_day_of_week', 'datavault4dbt')()) }}
{%- endmacro -%}

{%- macro default__first_day_of_week() %}

    {%- set global_var_iso = var('datavault4dbt.first_day_of_week_iso', none) -%}
    {%- set global_var_us = var('datavault4dbt.first_day_of_week_us', none) -%}
    
    {%- set first_day = 1 -%} 

    {%- if global_var_iso is mapping and target.type in global_var_iso.keys()|map('lower') -%}

        {%- set first_day = global_var_iso[target.type] -%}

    {%- elif global_var_us is mapping and target.type in global_var_us.keys()|map('lower') -%}
        
        {%- set first_day = global_var_us[target.type] -%}

    {%- else -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: Adapter '"~ target.type ~"' not found in either 'first_day_of_week_iso' or 'first_day_of_week_us' variables. Defaulting to 1.") -%}
        {%- endif -%}
    {%- endif -%}

    {{ return(first_day | int) }}

{%- endmacro -%}