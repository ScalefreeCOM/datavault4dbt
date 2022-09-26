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
                {%- set datatype = var('datavault4dbt.default_datatype_derived_columns', 'STRING') -%}
                {%- set value = column_value -%}

            {%- else -%}
            {# The value is an attribute and therefore the datatype gets detected out of the source relation. #}

                {%- set value = column_value -%}



                {%- set ns = namespace(datatype = "") -%}

                {%- for source_column in all_source_columns -%}

                    {%- if source_column.name|upper == value|upper -%}

                       {%- set ns.datatype = source_column.dtype -%}

                    {%- endif -%}

                {%- endfor -%}

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

            {%- do columns.update({column_name: {'datatype': datatype, 'value': value} }) -%}

        {%- elif column_value is mapping and not column_value.get('datatype') -%}

            {%- if execute -%}
                {{ exceptions.raise_compiler_error("Derived Column " + column_name + " is defined as a mapping, but has no datatype key set." ) }}
            {%- endif -%}

        {%- endif -%}

    {%- endfor -%}

{%- endif -%}

{{ return(columns | tojson) }}

{%- endmacro -%}
