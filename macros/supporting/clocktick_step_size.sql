{#
    Returns the clocktick step size as an integer.

    - If 'datavault4dbt.clocktick_step_size' is not set, the adapter-specific default_step_size is returned.
    - If it is set, it must be a positive integer (e.g. 1 or '1'). Anything else raises a compiler error.
    - If it is smaller than minimum_step_size, a compiler error is raised. Adapters use this when the database
      would round a smaller step down to zero (e.g. DATEADD with NANOSECOND on SQL Server, Synapse and Fabric).
#}
{%- macro clocktick_step_size(default_step_size=1, minimum_step_size=1) -%}

{%- set global_var = var('datavault4dbt.clocktick_step_size', none) -%}

{%- if global_var is none -%}
    {{ return(default_step_size) }}
{%- endif -%}

{%- set step_size_str = global_var | string | trim -%}
{%- set step_size = step_size_str | int(-1) -%}

{%- if step_size < 1 or step_size | string != step_size_str -%}
    {%- do exceptions.raise_compiler_error("Invalid value '" ~ global_var ~ "' for the global variable 'datavault4dbt.clocktick_step_size'. It must be a positive integer, e.g. 1.") -%}
{%- endif -%}

{%- if step_size < minimum_step_size -%}
    {%- do exceptions.raise_compiler_error("The global variable 'datavault4dbt.clocktick_step_size' is set to " ~ step_size ~ ", but the clocktick unit '" ~ datavault4dbt.clocktick_unit() | upper ~ "' needs a step size of at least " ~ minimum_step_size ~ " on " ~ target.type ~ ". A smaller step is rounded down to zero by the database, which makes the ledts equal to the next ldts.") -%}
{%- endif -%}

{{ return(step_size) }}

{%- endmacro -%}
