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