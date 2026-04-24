{%- macro as_constant(column_str=none) -%}

    {{- adapter.dispatch('as_constant', 'datavault4dbt')(column_str=column_str) -}}

{%- endmacro %}

{%- macro trino__as_constant(column_str) -%}

    {%- if column_str is not none and column_str is string and column_str -%}

        {%- if column_str | first == "!" -%}

            {{- return("'" ~ column_str[1:] ~ "'") -}}

        {%- else -%}

            {%- set sql_keywords = ['current_timestamp', 'current_date', 'current_time', 'localtime', 'localtimestamp'] -%}

            {%- if datavault4dbt.is_expression(column_str) or column_str | lower in sql_keywords -%}

                {{- return(column_str) -}}

            {%- else -%}

                {{- return(datavault4dbt.escape_column_names(column_str)) -}}

            {%- endif -%}

        {%- endif -%}
    {%- else -%}
        {%- if execute -%}
            {{ exceptions.raise_compiler_error("Invalid columns_str object provided. Must be a string and not null.") }}
        {%- endif %}
    {%- endif -%}

{%- endmacro -%}


{%- macro default__as_constant(column_str) -%}

    {%- if column_str is not none and column_str is string and column_str -%}

        {%- if column_str | first == "!" -%}
        
            {{- return("'" ~ column_str[1:] ~ "'") -}}
        
        {%- else -%}
        
            {%- if datavault4dbt.is_expression(column_str) -%}

                {{- return(column_str) -}}

            {%- else -%}

                {{- return(datavault4dbt.escape_column_names(column_str)) -}}

            {%- endif -%}

        {%- endif -%}
    {%- else -%}
        {%- if execute -%}
            {{ exceptions.raise_compiler_error("Invalid columns_str object provided. Must be a string and not null.") }}
        {%- endif %}
    {%- endif -%}

{%- endmacro -%}
