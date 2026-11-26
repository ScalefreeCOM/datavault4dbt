{%- macro subtract_clocktick(timestamp_expression) -%}

    {{ return(adapter.dispatch('subtract_clocktick', 'datavault4dbt')(timestamp_expression)) }}

{%- endmacro -%}


{%- macro default__subtract_clocktick(timestamp_expression) -%}

{%- set clocktick_unit = datavault4dbt.clocktick_unit() | upper -%}
{%- set clocktick_step_size = datavault4dbt.clocktick_step_size() -%}

TIMESTAMP_SUB({{ timestamp_expression }}, INTERVAL {{ clocktick_step_size }} {{ clocktick_unit }})

{%- endmacro -%}


{%- macro snowflake__subtract_clocktick(timestamp_expression) -%}

{%- set clocktick_unit = datavault4dbt.clocktick_unit() | upper -%}
{%- set clocktick_step_size = datavault4dbt.clocktick_step_size() -%}

DATEADD({{ clocktick_unit }}, -{{ clocktick_step_size }}, {{ timestamp_expression }})

{%- endmacro -%}


{%- macro exasol__subtract_clocktick(timestamp_expression) -%}

{%- set clocktick_unit = datavault4dbt.clocktick_unit() | upper -%}
{%- set clocktick_step_size = datavault4dbt.clocktick_step_size() -%}

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


{#- T-SQL DATEADD also accepts abbreviations of the datepart, e.g. ns for NANOSECOND.
    They become the full name, so that the NANOSECOND minimum step size applies to them too. -#}
{%- macro tsql_clocktick_datepart(clocktick_unit) -%}
{%- set abbreviations = {'NS': 'NANOSECOND', 'MCS': 'MICROSECOND', 'MS': 'MILLISECOND', 'SS': 'SECOND', 'S': 'SECOND', 'MI': 'MINUTE', 'N': 'MINUTE', 'HH': 'HOUR', 'DD': 'DAY', 'D': 'DAY'} -%}
{%- set datepart = clocktick_unit | string | upper -%}
{{ return(abbreviations.get(datepart, datepart)) }}
{%- endmacro -%}


{%- macro synapse__subtract_clocktick(timestamp_expression) -%}

{%- set clocktick_unit = datavault4dbt.tsql_clocktick_datepart(datavault4dbt.clocktick_unit()) -%}
{#- datetime2 / datetimeoffset stores at most 100 ns. DATEADD rounds a smaller NANOSECOND step down to zero. -#}
{%- if clocktick_unit == 'NANOSECOND' -%}
    {%- set clocktick_step_size = datavault4dbt.clocktick_step_size(default_step_size=100, minimum_step_size=100) -%}
{%- else -%}
    {%- set clocktick_step_size = datavault4dbt.clocktick_step_size() -%}
{%- endif -%}

DATEADD({{ clocktick_unit }}, -{{ clocktick_step_size }}, {{ timestamp_expression }})

{%- endmacro -%}


{%- macro postgres__subtract_clocktick(timestamp_expression) -%}

{%- set clocktick_unit = datavault4dbt.clocktick_unit() | lower -%}
{%- set clocktick_step_size = datavault4dbt.clocktick_step_size() -%}

{{ timestamp_expression }} - CAST('{{ clocktick_step_size }} {{ clocktick_unit }}' AS INTERVAL)

{%- endmacro -%}


{%- macro redshift__subtract_clocktick(timestamp_expression) -%}

{#- Interval subtraction instead of DATEADD: DATEADD has no TIMESTAMPTZ signature and returns TIMESTAMP.
    Redshift interval literals have no MICROSECOND or MILLISECOND unit, so these become fractional seconds
    (at most 6 digits): INTERVAL '00:00:00.000001' below one day, INTERVAL '<seconds>.<ffffff>' SECOND above. -#}
{%- set clocktick_unit = datavault4dbt.clocktick_unit() | upper -%}
{%- set clocktick_step_size = datavault4dbt.clocktick_step_size() -%}

{%- if clocktick_unit in ['MICROSECOND', 'MILLISECOND'] -%}
{%- set microseconds = clocktick_step_size * (1000 if clocktick_unit == 'MILLISECOND' else 1) -%}
{%- if microseconds < 86400000000 -%}
{#- Below one day: 'HH:MI:SS.ffffff', as in the package before the clocktick became configurable. -#}
{%- set seconds = microseconds // 1000000 -%}
{{ timestamp_expression }} - INTERVAL '{{ '%02d:%02d:%02d.%06d' | format(seconds // 3600, (seconds % 3600) // 60, seconds % 60, microseconds % 1000000) }}'
{%- else -%}
{{ timestamp_expression }} - INTERVAL '{{ '%d.%06d' | format(microseconds // 1000000, microseconds % 1000000) }}' SECOND
{%- endif -%}
{%- elif clocktick_unit in ['SECOND', 'MINUTE', 'HOUR', 'DAY'] -%}
{{ timestamp_expression }} - INTERVAL '{{ clocktick_step_size }}' {{ clocktick_unit }}
{%- else -%}
    {%- do exceptions.raise_compiler_error("Unsupported value '" ~ clocktick_unit ~ "' for datavault4dbt.clocktick_unit on Redshift. Supported values are MICROSECOND, MILLISECOND, SECOND, MINUTE, HOUR, and DAY.") -%}
{%- endif -%}

{%- endmacro -%}


{%- macro fabric__subtract_clocktick(timestamp_expression) -%}

{%- set clocktick_unit = datavault4dbt.tsql_clocktick_datepart(datavault4dbt.clocktick_unit()) -%}
{#- Fabric Warehouse datetime2 stores at most 1 microsecond. DATEADD rounds a smaller NANOSECOND step down to zero. -#}
{%- if clocktick_unit == 'NANOSECOND' -%}
    {%- set clocktick_step_size = datavault4dbt.clocktick_step_size(default_step_size=1000, minimum_step_size=1000) -%}
{%- else -%}
    {%- set clocktick_step_size = datavault4dbt.clocktick_step_size() -%}
{%- endif -%}

DATEADD({{ clocktick_unit }}, -{{ clocktick_step_size }}, {{ timestamp_expression }})

{%- endmacro -%}


{%- macro oracle__subtract_clocktick(timestamp_expression) -%}

{%- set clocktick_unit = datavault4dbt.clocktick_unit() | upper -%}
{%- set clocktick_step_size = datavault4dbt.clocktick_step_size() -%}

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
{%- set clocktick_step_size = datavault4dbt.clocktick_step_size() -%}

TRY_SUBTRACT({{ timestamp_expression }}, INTERVAL {{ clocktick_step_size }} {{ clocktick_unit }})

{%- endmacro -%}


{%- macro fabricspark__subtract_clocktick(timestamp_expression) -%}

{%- set clocktick_unit = datavault4dbt.clocktick_unit() | upper -%}
{%- set clocktick_step_size = datavault4dbt.clocktick_step_size() -%}

TRY_SUBTRACT({{ timestamp_expression }}, INTERVAL {{ clocktick_step_size }} {{ clocktick_unit }})

{%- endmacro -%}


{%- macro trino__subtract_clocktick(timestamp_expression) -%}

{%- set clocktick_unit = datavault4dbt.clocktick_unit() | lower -%}
{%- set clocktick_step_size = datavault4dbt.clocktick_step_size() -%}

date_add('{{ clocktick_unit }}', -{{ clocktick_step_size }}, {{ timestamp_expression }})

{%- endmacro -%}


{%- macro sqlserver__subtract_clocktick(timestamp_expression) -%}

{%- set clocktick_unit = datavault4dbt.tsql_clocktick_datepart(datavault4dbt.clocktick_unit()) -%}
{#- datetime2 / datetimeoffset stores at most 100 ns. DATEADD rounds a smaller NANOSECOND step down to zero. -#}
{%- if clocktick_unit == 'NANOSECOND' -%}
    {%- set clocktick_step_size = datavault4dbt.clocktick_step_size(default_step_size=100, minimum_step_size=100) -%}
{%- else -%}
    {%- set clocktick_step_size = datavault4dbt.clocktick_step_size() -%}
{%- endif -%}

DATEADD({{ clocktick_unit }}, -{{ clocktick_step_size }}, {{ timestamp_expression }})

{%- endmacro -%}
