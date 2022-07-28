{%- macro derive_columns(source_relation=none, columns=none) -%}

    {{- adapter.dispatch('derive_columns', 'dbtvault_scalefree')(source_relation=source_relation, columns=columns) -}}

{%- endmacro %}

{%- macro default__derive_columns(source_relation=none, columns=none) -%}

{%- set exclude_columns = [] -%}
{%- set include_columns = [] -%}
{%- set src_columns = [] -%}
{%- set der_columns = [] -%}

{%- set source_cols = dbtvault_scalefree.source_columns(source_relation=source_relation) -%}

{%- if columns is mapping and columns is not none -%}

    {#- Add aliases of derived columns to excludes and full SQL to includes -#}
    {%- for col in columns -%}

        {%- if dbtvault_scalefree.is_list(columns[col]['value']) -%}
            {%- set column_list = [] -%}

            {%- for concat_component in columns[col]['value'] -%}
                {%- set column_str = dbtvault_scalefree.as_constant(concat_component) -%}
                {%- do column_list.append(column_str) -%}
            {%- endfor -%}
            {%- set concat = dbtvault_scalefree.concat_ws(column_list, "||") -%}
            {%- set concat_string = concat ~ " AS " ~ dbtvault_scalefree.escape_column_names(col) -%}

            {%- do der_columns.append(concat_string) -%}
            {%- set exclude_columns = exclude_columns + columns[col]['value'] -%}
        {% else %}
            {%- set column_str = dbtvault_scalefree.as_constant(columns[col]['value']) -%}
            {%- do der_columns.append(column_str ~ " AS " ~ dbtvault_scalefree.escape_column_names(col)) -%}
            {%- do exclude_columns.append(col) -%}
        {% endif %}

    {%- endfor -%}

    {#- Add all columns from source_model relation -#}
    {%- if source_relation is defined and source_relation is not none -%}

        {%- for col in source_cols -%}
            {%- if col not in exclude_columns -%}
                {%- do src_columns.append(dbtvault_scalefree.escape_column_names(col)) -%}
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


{%- macro derived_columns_datatypes(derived_columns, source_relation) -%}

    {{- adapter.dispatch('derived_columns_datatypes', 'dbtvault_scalefree')(derived_columns=derived_columns, source_relation=source_relation) -}}

{%- endmacro -%}


{%- macro default__derived_columns_datatypes(derived_columns, source_relation) -%}

{%- set source_columns = adapter.get_columns_in_relation(source_relation) -%}

{%- for column_name, column_value in derived_columns.items() -%}

    {%- if not (column_value is mapping and column_value.datatype is not none) and column_value is string -%}
    {# This is the case where no datatype is defined and one must be detected based on the input value. #}

        {%- if not dbtvault_scalefree.is_attribute(column_value) -%}
        {# If the value is a static value, it is not an attribute and no datatype needs to be detected. Instead a default datatype is applied. #}
            
            {%- set datatype = var('dbtvault_scalefree.default_datatype_derived_columns', 'STRING') -%}

        {%- else -%}
        {# The value is an attribute and therefore the datatype gets detected out of the source relation. #}

            {%- set input_column = column_value -%}

            {%- set ns = namespace(datatype = "") -%}

            {%- for source_column in source_columns -%}

                {%- if source_column.name == input_column -%}

                    {%- set ns.datatype = source_column.dtype -%}
                
                {%- endif -%}

            {%- endfor -%}

            {%- if ns.datatype != "" -%}

                {%- set datatype = ns.datatype -%}

            {%- else -%}
            {# The input column name could not be found inside the source relation. #}

                {%- if execute -%}
                    {{ exceptions.raise_compiler_error("Could not find the derived_column input column " + input_column + " inside the source relation " + source_relation|string ) }}
                {%- endif -%}

            {%- endif -%}

        {%- endif -%}
            
        {%- do derived_columns.update({column_name: {'datatype': datatype}}) -%}

    {%- elif column_value is mapping and column_value.datatype is none -%}

        {%- if execute -%}
            {{ exceptions.raise_compiler_error("Derived Column " + column_name + " is defined as a mapping, but has no datatype key set." ) }}
        {%- endif -%}

    {%- endif -%}

{%- endfor -%}

{{ return(derived_columns) }}

{%- endmacro -%}

