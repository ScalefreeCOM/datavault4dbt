{%- macro numeric_ghost_value(datatype, sentinel_value) -%}

{# A negative sentinel overflows a fully fractional type like DECIMAL(2,2) #}
{%- if '(' in datatype and ',' in datatype -%}
    {%- set type_parameters = datatype.split('(')[1].split(')')[0].split(',') -%}
    {%- if (type_parameters[0]|trim|int) <= (type_parameters[1]|trim|int) -%}
        {%- do return(0) -%}
    {%- endif -%}
{%- endif -%}

{%- do return(sentinel_value) -%}

{%- endmacro -%}
