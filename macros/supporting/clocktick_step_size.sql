{#
    Returns the clocktick step size as an integer.

    - If 'datavault4dbt.clocktick_step_size' is not set, the adapter-specific default_step_size is returned.
    - It takes one value for all adapters, or a dictionary with the adapter name as key, like
      'datavault4dbt.clocktick_unit'. A dictionary without a key for the current adapter returns
      default_step_size, with a warning.
    - The value must be a positive integer (e.g. 1 or '1'). Anything else raises a compiler error.
    - If it is smaller than minimum_step_size, a compiler error is raised. Adapters use this when the database
      would round a smaller step down to zero (e.g. DATEADD with NANOSECOND on SQL Server, Synapse and Fabric).
#}
{%- macro clocktick_step_size(default_step_size=1, minimum_step_size=1) -%}

{%- set global_var = var('datavault4dbt.clocktick_step_size', none) -%}

{%- if global_var is mapping -%}
    {%- set adapter_value = namespace(found=false, value=none) -%}
    {%- for key, value in global_var.items() -%}
        {%- if key | string | lower == target.type | lower -%}
            {%- set adapter_value.found = true -%}
            {%- set adapter_value.value = value -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if not adapter_value.found -%}
        {%- if execute -%}
            {%- do exceptions.warn("Warning: You have set the global variable 'datavault4dbt.clocktick_step_size' to a dictionary, but have not included the adapter you use (" ~ target.type ~ ") as a key. Applying the default value.") -%}
        {%- endif -%}
        {{ return(default_step_size) }}
    {%- endif -%}
    {%- set global_var = adapter_value.value -%}
{%- endif -%}

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
