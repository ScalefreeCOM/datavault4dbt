{%- macro parameterize_numeric_datatype(datatype, numeric_precision=none, numeric_scale=none) -%}

{%- set upper_datatype = datatype|string|upper|trim -%}

{# DATETIME2/DATETIMEOFFSET report their fractional-seconds precision as numeric_scale too #}
{%- if ('DATETIME2' in upper_datatype or 'DATETIMEOFFSET' in upper_datatype)
        and '(' not in upper_datatype
        and numeric_scale is not none and numeric_scale != '' -%}
    {%- do return(datatype ~ "(" ~ (numeric_scale|int|string) ~ ")") -%}
{%- endif -%}

{# Restricted to DECIMAL/NUMERIC because types like INT report a precision too, but cannot carry one #}
{%- set is_parameterizable = ('DECIMAL' in upper_datatype or 'NUMERIC' in upper_datatype) and '(' not in upper_datatype -%}

{%- if is_parameterizable and numeric_precision is not none and numeric_precision != '' -%}
    {%- if numeric_scale is not none and numeric_scale != '' -%}
        {%- do return(datatype ~ "(" ~ (numeric_precision|int|string) ~ "," ~ (numeric_scale|int|string) ~ ")") -%}
    {%- else -%}
        {%- do return(datatype ~ "(" ~ (numeric_precision|int|string) ~ ")") -%}
    {%- endif -%}
{%- endif -%}

{%- do return(datatype) -%}

{%- endmacro -%}
