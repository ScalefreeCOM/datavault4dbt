{%- macro escape_column_names(columns=none) -%}

{# Different platforms use different escape characters, the default below is for Snowflake which uses double quotes #}

    {%- if datavault4dbt.is_something(columns) -%}

        {%- set col_string = '' -%}
        {%- set col_list = [] -%}
        {%- set col_mapping = {} -%}

        {%- if columns is string -%}

            {%- set col_string = datavault4dbt.escape_column_name(columns) -%}

        {%- elif datavault4dbt.is_list(columns) -%}

            {%- for col in columns -%}

                {%- if col is string -%}

                    {%- set escaped_col = datavault4dbt.escape_column_name(col) -%}

                    {%- do col_list.append(escaped_col) -%}

                {%- else -%}

                    {%- if execute -%}
                        {{- exceptions.raise_compiler_error("Invalid column name(s) provided. Must be a string.") -}}
                    {%- endif -%}

                {%- endif -%}

            {%- endfor -%}

        {%- elif columns is mapping -%}

            {%- if columns['source_column'] and columns['alias'] -%}

                {%- set escaped_source_col = datavault4dbt.escape_column_name(columns['source_column']) -%}
                {%- set escaped_alias_col = datavault4dbt.escape_column_name(columns['alias']) -%}
                {%- set col_mapping = {"source_column": escaped_source_col, "alias": escaped_alias_col} -%}

            {%- else -%}

                {%- if execute -%}
                    {{- exceptions.raise_compiler_error("Invalid column name(s) provided. Must be a string, a list of strings, or a dictionary of hashdiff metadata.") -}}
                {%- endif %}

            {%- endif -%}

        {%- else -%}

            {%- if execute -%}
                {{- exceptions.raise_compiler_error("Invalid column name(s) provided. Must be a string, a list of strings, or a dictionary of hashdiff metadata.") -}}
            {%- endif %}

        {%- endif -%}

    {%- elif columns == '' -%}

        {%- if execute -%}
            {{- exceptions.raise_compiler_error("Expected a column name or a list of column names, got an empty string") -}}
        {%- endif -%}

    {%- endif -%}

{%- if columns is none -%}

    {%- do return(none) -%}

{%- elif columns == [] -%}

    {%- do return([]) -%}

{%- elif columns == {} -%}

    {%- do return({}) -%}

{%- elif columns is string -%}

    {%- do return(col_string) -%}

{%- elif datavault4dbt.is_list(columns) -%}

    {%- do return(col_list) -%}

{%- elif columns is mapping -%}

    {%- do return(col_mapping) -%}

{%- endif -%}

{%- endmacro -%}


{%- macro escape_column_name(column) -%}

    {{- adapter.dispatch('escape_column_name', 'datavault4dbt')(column=column) -}}

{%- endmacro %}


{%- macro default__escape_column_name(column) -%}

    {%- set escape_char_left  = var('escape_char_left',  '"') -%}
    {%- set escape_char_right = var('escape_char_right', '"') -%}

    {%- set escaped_column_name = escape_char_left ~ column | upper | replace(escape_char_left, '') | replace(escape_char_right, '') | trim ~ escape_char_right | indent(4) -%}

    {%- do return(escaped_column_name) -%}

{%- endmacro -%}


{%- macro synapse__escape_column_name(column) -%}

    {%- set escape_char_left  = var('escape_char_left',  '"') -%}
    {%- set escape_char_right = var('escape_char_right', '"') -%}

    {%- set escaped_column_name = escape_char_left ~ column | replace(escape_char_left, '') | replace(escape_char_right, '') | trim ~ escape_char_right -%}

    {%- do return(escaped_column_name) -%}

{%- endmacro -%}

{%- macro bigquery__escape_column_name(column) -%}

    {%- set escape_char_left  = var('escape_char_left',  '`') -%}
    {%- set escape_char_right = var('escape_char_right', '`') -%}

    {%- set escaped_column_name = escape_char_left ~ column | replace(escape_char_left, '') | replace(escape_char_right, '') | trim ~ escape_char_right -%}

    {%- do return(escaped_column_name) -%}

{%- endmacro -%}

{%- macro postgres__escape_column_name(column) -%}

    {%- set escape_char_left  = var('escape_char_left',  "") -%}
    {%- set escape_char_right = var('escape_char_right', "") -%}

    {%- set escaped_column_name = escape_char_left ~ column | lower | replace(escape_char_left, '') | replace(escape_char_right, '') | trim ~ escape_char_right | indent(4) -%}

    {%- do return(escaped_column_name) -%}

{%- endmacro -%}

{%- macro redshift__escape_column_name(column) -%}

    {%- set escape_char_left  = var('escape_char_left',  '"') -%}
    {%- set escape_char_right = var('escape_char_right', '"') -%}

    {%- set escaped_column_name = escape_char_left ~ column | lower | replace(escape_char_left, '') | replace(escape_char_right, '') | trim ~ escape_char_right | indent(4) -%}

    {%- do return(escaped_column_name) -%}

{%- endmacro -%}

{%- macro exasol__escape_column_name(column) -%}

    {%- set escape_char_left  = var('escape_char_left',  '') -%}
    {%- set escape_char_right = var('escape_char_right', '') -%}

    {%- set escaped_column_name = escape_char_left ~ column | upper | replace(escape_char_left, '') | replace(escape_char_right, '') | trim ~ escape_char_right | indent(4) -%}

    {%- do return(escaped_column_name) -%}

{%- endmacro -%}


{%- macro fabric__escape_column_name(column) -%}

    {%- set escape_char_left  = var('escape_char_left',  '"') -%}
    {%- set escape_char_right = var('escape_char_right', '"') -%}

    {%- set escaped_column_name = escape_char_left ~ column | replace(escape_char_left, '') | replace(escape_char_right, '') | trim ~ escape_char_right -%}

    {% set set_casing = var('datavault4dbt.set_casing', none) %}
    {% if set_casing|lower in ['upper', 'uppercase'] %}
        {%- set escaped_column_name = escaped_column_name | upper -%}
    {% elif set_casing|lower in ['lower', 'lowercase'] %}
        {%- set escaped_column_name = escaped_column_name | lower -%}
    {% endif %}

    {%- do return(escaped_column_name) -%}

{%- endmacro -%}


{%- macro databricks__escape_column_name(column) -%}

    {%- set escape_char_left  = var('escape_char_left',  "") -%}
    {%- set escape_char_right = var('escape_char_right', "") -%}

    {%- set escaped_column_name = escape_char_left ~ column | replace(escape_char_left, '') | replace(escape_char_right, '') | trim ~ escape_char_right | indent(4) -%}

    {% set set_casing = var('datavault4dbt.set_casing', none) %}
    {% if set_casing|lower in ['upper', 'uppercase'] %}
        {%- set escaped_column_name = escaped_column_name | upper -%}
    {% elif set_casing|lower in ['lower', 'lowercase'] %}
        {%- set escaped_column_name = escaped_column_name | lower -%}
    {% endif %}

    {%- do return(escaped_column_name) -%}

{%- endmacro -%}