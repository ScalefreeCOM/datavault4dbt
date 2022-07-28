{%- macro derived_columns_datatypes(derived_columns, all_source_columns) -%}

    {{- adapter.dispatch('derived_columns_datatypes', 'dbtvault_scalefree')(derived_columns=derived_columns, all_source_columns=all_source_columns) -}}

{%- endmacro -%}


{%- macro default__derived_columns_datatypes(derived_columns, all_source_columns) -%}

{{ log('Macro derived_columns_datatypes called by model ' + this|string + '. Derived Columns = ' + derived_columns|string, true) }}

{%- if derived_column is mapping -%}
    {{ log('Derived Columns is of type mapping!', true) }}
{%- else -%}
    {{ log('Derived Columns is not of type mapping!', true) }}
{%- endif -%}

{%- if derived_columns is not mapping and derive_columns is string -%}
    {%- if execute -%}
        {{ exceptions.raise_compiler_error("Derived Columns is of datatype string. Needs to be mapping" ) }}
    {%- endif -%}

{%- else -%}

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

                {%- for source_column in all_source_columns -%}

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

{%- endif -%}

{{ return(derived_columns) }}

{%- endmacro -%}

