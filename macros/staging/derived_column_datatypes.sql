{%- macro derived_columns_datatypes(columns, source_relation) -%}

    {{- adapter.dispatch('derived_columns_datatypes', 'datavault4dbt')(columns=columns, source_relation=source_relation) -}}

{%- endmacro -%}


{%- macro default__derived_columns_datatypes(columns, source_relation) -%}

{%- set all_source_columns = adapter.get_columns_in_relation(source_relation) -%}

{%- if columns is not mapping and columns is string -%}
    {%- if execute -%}
        {{ exceptions.raise_compiler_error("Derived Columns is of datatype string. Needs to be mapping" ) }}
    {%- endif -%}

{%- else -%}

    {%- for column_name, column_value in columns.items() -%}

        {%- if not (column_value is mapping) and column_value is string -%}
        {# This is the case where no datatype is defined and one must be detected based on the input value. #}

            {%- if not datavault4dbt.is_attribute(column_value) -%}
            {# If the value is a static value, it is not an attribute and no datatype needs to be detected. Instead a default datatype is applied. #}

                {%- set datatype = var('datavault4dbt.derived_columns_default_dtype', 'STRING') -%}
                {%- set value = column_value -%}
                {%- set col_size = "" -%}

            {%- else -%}
            {# The value is an attribute and therefore the datatype gets detected out of the source relation. #}

                {%- set value = column_value -%}

                {%- set ns = namespace(datatype = "", col_size="") -%}

                {%- for source_column in all_source_columns -%}
                    {%- if source_column.name|upper == value|upper -%}

                        {%- set ns.datatype = source_column.dtype -%}

                        {% if datavault4dbt.is_something(source_column.char_size) %}
                            {%- set ns.col_size = source_column.char_size -%}
                        {%- endif -%}
                    {%- endif -%}

                {%- endfor -%}
                {%- set col_size = ns.col_size | int-%}
                {%- if ns.datatype != "" -%}

                    {%- set datatype = ns.datatype -%}

                {%- else -%}
                {# The input column name could not be found inside the source relation. #}

                    {%- if execute -%}
                        {{ exceptions.raise_compiler_error("Could not find the derived_column input column " + value + " inside the source relation " + source_relation|string ) }}
                    {%- else -%}
                        {%- set datatype = "" -%}
                    {%- endif -%}

                {%- endif -%}

            {%- endif -%}

            {%- do columns.update({column_name: {'datatype': datatype, 'value': value, 'col_size': col_size} }) -%}
        
        {%- elif column_value is mapping and not column_value.get('datatype') -%}

                {%- set value = column_value['value'] -%}

                {%- set ns = namespace(datatype = "", col_size="") -%}

                {%- for source_column in all_source_columns -%}

                    {%- if source_column.name|upper == value|upper -%}

                        {%- set ns.datatype = source_column.dtype -%}

                        {% if datavault4dbt.is_something(source_column.char_size) %}
                            {%- set ns.col_size = source_column.char_size -%}
                        {%- endif -%}
                    {%- endif -%}

                {%- endfor -%}

                {%- if ns.datatype != "" -%}

                    {%- set datatype = ns.datatype -%}

                {%- else -%}
                {# The input column name could not be found inside the source relation. #}

                    {%- if execute -%}
                        {{ exceptions.raise_compiler_error("Could not find the derived_column input column " + value + " inside the source relation " + source_relation|string + ". Try setting it manually with the key 'datatype'." ) }}
                    {%- else -%}
                        {%- set datatype = "" -%}
                        {%- set col_size = "" -%}

                    {%- endif -%}

                {%- endif -%}
                {%- set col_size = ns.col_size | int-%}
                {%- do columns.update({column_name: {'datatype': datatype, 'value': value, "col_size": col_size} }) -%}
        {%- elif column_value is mapping and not column_value.get('col_size') -%}

            {%- set value = column_value['value'] -%}
            {%- set datatype = column_value['datatype'] -%}
            {%- set ns = namespace(col_size = "") -%}

            {%- for source_column in all_source_columns -%}

                {%- if source_column.name|upper == value|upper -%}

                    {%- set ns.col_size = source_column.char_size | int -%}
                {%- endif -%}

            {%- endfor -%}

            {%- set col_size = ns.col_size -%}

            {%- do columns.update({column_name: {'datatype': datatype, 'value': value, 'col_size': col_size} }) -%}
        {%- endif -%}

    {%- endfor -%}

{%- endif -%}

{{ return(columns | tojson) }}

{%- endmacro -%}