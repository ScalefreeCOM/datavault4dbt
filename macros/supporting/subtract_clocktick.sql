{%- macro subtract_clocktick(timestamp_expression) -%}

    {{ return(adapter.dispatch('subtract_clocktick', 'datavault4dbt')(timestamp_expression)) }}

{%- endmacro -%}


{%- macro default__subtract_clocktick(timestamp_expression) -%}

{%- set clocktick_unit = datavault4dbt.clocktick_unit() | upper -%}
{%- set clocktick_step_size = var('datavault4dbt.clocktick_step_size', '1') -%}

TIMESTAMP_SUB({{ timestamp_expression }}, INTERVAL {{ clocktick_step_size }} {{ clocktick_unit }})

{%- endmacro -%}


{%- macro snowflake__subtract_clocktick(timestamp_expression) -%}

{%- set clocktick_unit = datavault4dbt.clocktick_unit() | upper -%}
{%- set clocktick_step_size = var('datavault4dbt.clocktick_step_size', '1') -%}

DATEADD({{ clocktick_unit }}, -{{ clocktick_step_size }}, {{ timestamp_expression }})

{%- endmacro -%}


{%- macro exasol__subtract_clocktick(timestamp_expression) -%}

{%- set clocktick_unit = datavault4dbt.clocktick_unit() | upper -%}
{%- set clocktick_step_size = var('datavault4dbt.clocktick_step_size', '1') -%}

{%- if clocktick_unit == 'MICROSECOND' -%}
ADD_SECONDS({{ timestamp_expression }}, -({{ clocktick_step_size }} / 1000000.0))
{%- elif clocktick_unit == 'MILLISECOND' -%}
ADD_SECONDS({{ timestamp_expression }}, -({{ clocktick_step_size }} / 1000.0))
{%- elif clocktick_unit == 'SECOND' -%}
ADD_SECONDS({{ timestamp_expression }}, -{{ clocktick_step_size }})
{%- elif clocktick_unit == 'MINUTE' -%}
ADD_MINUTES({{ timestamp_expression }}, -{{ clocktick_step_size }})
{%- elif clocktick_unit == 'HOUR' -%}
ADD_HOURS({{ timestamp_expression }}, -{{ clocktick_step_size }})
{%- elif clocktick_unit == 'DAY' -%}
ADD_DAYS({{ timestamp_expression }}, -{{ clocktick_step_size }})
{%- else -%}
    {%- do exceptions.raise_compiler_error("Unsupported value '" ~ clocktick_unit ~ "' for datavault4dbt.clocktick_unit on Exasol. Supported values are MICROSECOND, MILLISECOND, SECOND, MINUTE, HOUR, and DAY.") -%}
{%- endif -%}

{%- endmacro -%}


{%- macro synapse__subtract_clocktick(timestamp_expression) -%}

{%- set clocktick_unit = datavault4dbt.clocktick_unit() | upper -%}
{%- if clocktick_unit == 'NANOSECOND' and var('datavault4dbt.clocktick_step_size', '1')|int < 100 -%}
    {%- set clocktick_step_size = 100 -%}
{%- else -%}
    {%- set clocktick_step_size = var('datavault4dbt.clocktick_step_size', '1') -%}
{%- endif -%}

DATEADD({{ clocktick_unit }}, -{{ clocktick_step_size }}, {{ timestamp_expression }})

{%- endmacro -%}


{%- macro postgres__subtract_clocktick(timestamp_expression) -%}

{%- set clocktick_unit = datavault4dbt.clocktick_unit() | lower -%}
{%- set clocktick_step_size = var('datavault4dbt.clocktick_step_size', '1') -%}

{{ timestamp_expression }} - CAST('{{ clocktick_step_size }} {{ clocktick_unit }}' AS INTERVAL)

{%- endmacro -%}


{%- macro redshift__subtract_clocktick(timestamp_expression) -%}

{%- set clocktick_unit = datavault4dbt.clocktick_unit() | upper -%}
{%- set clocktick_step_size = var('datavault4dbt.clocktick_step_size', '1') -%}

DATEADD({{ clocktick_unit }}, -{{ clocktick_step_size }}, {{ timestamp_expression }})

{%- endmacro -%}


{%- macro fabric__subtract_clocktick(timestamp_expression) -%}

{%- set clocktick_unit = datavault4dbt.clocktick_unit() | upper -%}
{%- if clocktick_unit == 'NANOSECOND' and var('datavault4dbt.clocktick_step_size', '1')|int < 1000 -%}
    {%- set clocktick_step_size = 1000 -%}
{%- else -%}
    {%- set clocktick_step_size = var('datavault4dbt.clocktick_step_size', '1') -%}
{%- endif -%}

DATEADD({{ clocktick_unit }}, -{{ clocktick_step_size }}, {{ timestamp_expression }})

{%- endmacro -%}


{%- macro oracle__subtract_clocktick(timestamp_expression) -%}

{%- set clocktick_unit = datavault4dbt.clocktick_unit() | upper -%}
{%- set clocktick_step_size = var('datavault4dbt.clocktick_step_size', '1') -%}

{%- if clocktick_unit == 'MICROSECOND' -%}
{{ timestamp_expression }} - NUMTODSINTERVAL({{ clocktick_step_size }} / 1000000.0, 'SECOND')
{%- elif clocktick_unit == 'MILLISECOND' -%}
{{ timestamp_expression }} - NUMTODSINTERVAL({{ clocktick_step_size }} / 1000.0, 'SECOND')
{%- elif clocktick_unit in ['SECOND', 'MINUTE', 'HOUR', 'DAY'] -%}
{{ timestamp_expression }} - NUMTODSINTERVAL({{ clocktick_step_size }}, '{{ clocktick_unit }}')
{%- else -%}
    {%- do exceptions.raise_compiler_error("Unsupported value '" ~ clocktick_unit ~ "' for datavault4dbt.clocktick_unit on Oracle. Supported values are MICROSECOND, MILLISECOND, SECOND, MINUTE, HOUR, and DAY.") -%}
{%- endif -%}

{%- endmacro -%}


{%- macro databricks__subtract_clocktick(timestamp_expression) -%}

{%- set clocktick_unit = datavault4dbt.clocktick_unit() | upper -%}
{%- set clocktick_step_size = var('datavault4dbt.clocktick_step_size', '1') -%}

TRY_SUBTRACT({{ timestamp_expression }}, INTERVAL {{ clocktick_step_size }} {{ clocktick_unit }})

{%- endmacro -%}


{%- macro trino__subtract_clocktick(timestamp_expression) -%}

{%- set clocktick_unit = datavault4dbt.clocktick_unit() | lower -%}
{%- set clocktick_step_size = var('datavault4dbt.clocktick_step_size', '1') -%}

date_add('{{ clocktick_unit }}', -{{ clocktick_step_size }}, {{ timestamp_expression }})

{%- endmacro -%}


{%- macro sqlserver__subtract_clocktick(timestamp_expression) -%}

{%- set clocktick_unit = datavault4dbt.clocktick_unit() | upper -%}
{%- if clocktick_unit == 'NANOSECOND' and var('datavault4dbt.clocktick_step_size', '1')|int < 100 -%}
    {%- set clocktick_step_size = 100 -%}
{%- else -%}
    {%- set clocktick_step_size = var('datavault4dbt.clocktick_step_size', '1') -%}
{%- endif -%}

DATEADD({{ clocktick_unit }}, -{{ clocktick_step_size }}, {{ timestamp_expression }})

{%- endmacro -%}