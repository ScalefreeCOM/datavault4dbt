{%- macro derive_columns(source_relation=none, columns=none) -%}

    {{- adapter.dispatch('derive_columns', 'datavault4dbt')(source_relation=source_relation, columns=columns) -}}

{%- endmacro %}

{%- macro default__derive_columns(source_relation=none, columns=none) -%}

{%- set exclude_columns = [] -%}
{%- set include_columns = [] -%}
{%- set src_columns = [] -%}
{%- set der_columns = [] -%}

{%- set source_cols = datavault4dbt.source_columns(source_relation=source_relation) -%}

{%- if columns is mapping and columns is not none -%}

    {#- Add aliases of derived columns to exclude and full SQL to include -#}
    {%- for col in columns -%}

        {%- if datavault4dbt.is_list(columns[col]['value']) -%}
            {%- set column_list = [] -%}

            {%- for concat_component in columns[col]['value'] -%}
                {%- set column_str = datavault4dbt.as_constant(concat_component) -%}
                {%- do column_list.append(column_str) -%}
            {%- endfor -%}
            {%- set concat = datavault4dbt.concat_ws(column_list, "||") -%}
            {%- set concat_string = concat ~ " AS " ~ datavault4dbt.escape_column_names(col) -%}

            {%- do der_columns.append(concat_string) -%}
            {%- set exclude_columns = exclude_columns + columns[col]['value'] -%}
        {% else %}
            {%- set column_str = datavault4dbt.as_constant(columns[col]['value']) -%}
            {%- do der_columns.append(column_str ~ " AS " ~ datavault4dbt.escape_column_names(col)) -%}
            {%- do exclude_columns.append(col) -%}
        {% endif %}

    {%- endfor -%}

    {#- Add all columns from source_model relation -#}
    {%- if source_relation is defined and source_relation is not none -%}

        {%- for col in source_cols -%}
            {%- if col not in exclude_columns -%}
                {%- do src_columns.append(datavault4dbt.escape_column_names(col)) -%}
            {%- endif -%}
        {%- endfor -%}

    {%- endif -%}

    {#- Makes sure the columns are appended in a logical order. Source columns then derived columns -#}
    {%- set include_columns = src_columns + der_columns -%}

        {#- Print out all columns in includes -#}
        {%- for col in include_columns -%}
            {{- col | indent(4) -}}{{ ",\n" if not loop.last }}
        {%- endfor -%}
    {%- else -%}
        {%- if execute -%}
            {{ exceptions.raise_compiler_error("Invalid column configuration:
            expected format: {'source_relation': Relation, 'columns': {column_name: column_value}}
            got: {'source_relation': " ~ source_relation ~ ", 'columns': " ~ columns ~ "}") }}
        {%- endif %}
    {%- endif %}
{%- endmacro -%}
