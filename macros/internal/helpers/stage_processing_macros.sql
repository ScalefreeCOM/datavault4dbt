{%- macro process_columns_to_select(columns_list=none, exclude_columns_list=none) -%}
    {% set exclude_columns_list = exclude_columns_list | map('upper') | list %}
    {% set columns_list = columns_list | map('upper') | list %}
    {% set columns_to_select = [] %}

    {% if not datavault4dbt.is_list(columns_list) or not datavault4dbt.is_list(exclude_columns_list)  %}

        {{- exceptions.raise_compiler_error("One or both arguments are not of list type.") -}}

    {%- endif -%}

    {%- if datavault4dbt.is_something(columns_list) and datavault4dbt.is_something(exclude_columns_list) -%}
        {%- for col in columns_list -%}

            {%- if col not in exclude_columns_list -%}
                {%- do columns_to_select.append(col) -%}
            {%- endif -%}

        {%- endfor -%}
    {%- elif datavault4dbt.is_something(columns_list) and not datavault4dbt.is_something(exclude_columns_list) %}
        {% set columns_to_select = columns_list %}
    {%- endif -%}

    {%- do return(columns_to_select) -%}

{%- endmacro -%}


{%- macro extract_column_names(columns_dict=none) -%}

    {%- set extracted_column_names = [] -%}

    {%- if columns_dict is mapping -%}
        {%- for key, value in columns_dict.items() -%}
            {%- do extracted_column_names.append(key) -%}
        {%- endfor -%}

        {%- do return(extracted_column_names) -%}
    {%- else -%}
        {%- do return([]) -%}
    {%- endif -%}

{%- endmacro -%}

{%- macro extract_input_columns(columns_dict=none) -%}

    {%- set extracted_input_columns = [] -%}

    {%- if columns_dict is mapping -%}
        {%- for key, value in columns_dict.items() -%}
            {%- if value is mapping and 'src_cols_required' in value.keys() -%}
                {%- do extracted_input_columns.append(value['src_cols_required']) -%}
            {%- elif value is mapping and 'value' in value.keys() and 'src_cols_required' not in value.keys() -%}
                {# Do nothing. No source column required. #}    
            {%- elif value is mapping and value.is_hashdiff -%}
                {%- do extracted_input_columns.append(value['columns']) -%}
            {%- elif value is mapping and 'this_column_name' in value.keys() -%}
                {%- if datavault4dbt.is_list(value['this_column_name'])-%}
                    {%- for column in value['this_column_name'] -%}
                        {%- do extracted_input_columns.append(column) -%}
                    {%- endfor -%}
                {%- else -%}
                    {%- do extracted_input_columns.append(value['this_column_name']) -%}
                {%- endif -%}
            {%- else -%}
                {%- do extracted_input_columns.append(value) -%}
            {%- endif -%}
        {%- endfor -%}

        {%- do return(extracted_input_columns) -%}
    {%- else -%}
        {%- do return([]) -%}
    {%- endif -%}

{%- endmacro -%}


{%- macro exclude_hashdiff_columns(source_model=none,hash_columns=none) -%}
    {# Get all source columns #}
    {#- Check for source format or ref format and create relation object from source_model -#}
    {% if source_model is mapping and source_model is not none -%}

        {%- set source_name = source_model | first -%}
        {%- set source_table_name = source_model[source_name] -%}

        {%- set source_relation = source(source_name, source_table_name) -%}
        {%- set all_source_columns = datavault4dbt.source_columns(source_relation=source_relation) -%}

    {%- elif source_model is not mapping and source_model is not none -%}

        {{ log('source_model is not mapping and not none: ' ~ source_model, false) }}

        {%- set source_relation = ref(source_model) -%}
        {%- set all_source_columns = datavault4dbt.source_columns(source_relation=source_relation) -%}
    {%- else -%}
        {%- set all_source_columns = [] -%}
    {%- endif -%}

    {{ log('source_relation: ' ~ source_relation, false) }}

    {# Exclude columns #}
    {%- set processed_hash_columns = {} -%}
    {%- for col, col_mapping in hash_columns.items() -%} 
        {%- if col_mapping is mapping -%}
            {% if ('columns' in col_mapping.keys()) and ('exclude_columns' in col_mapping.keys()) %}
                {{- exceptions.raise_compiler_error("hashed_columns: You can only use 'columns' or 'exclude_columns'.") -}}
            {%- elif 'exclude_columns' in col_mapping.keys() -%}
                {%- set columns_to_hash = datavault4dbt.process_columns_to_select(all_source_columns, col_mapping.exclude_columns) -%}
                {%- do hash_columns[col].pop('exclude_columns') -%}
                {%- do hash_columns[col].update({'columns': columns_to_hash}) -%}
                {%- do processed_hash_columns.update({col: hash_columns[col]}) -%}
            {%- else -%}
                {%- do processed_hash_columns.update({col: hash_columns[col]}) -%}
            {%- endif -%}
        {%- else -%}
            {%- do processed_hash_columns.update({col: col_mapping}) -%}
        {%- endif -%}
    {%- endfor -%}
{{ log('processed_hash_columns: ' ~ processed_hash_columns, info=True) }}
    {%- do return(processed_hash_columns) -%}

{%- endmacro -%}


{%- macro print_list(list_to_print=none, indent=4, src_alias=none) -%}

    {%- for col_name in list_to_print -%}
        {%- if src_alias %}
        {{ (src_alias ~ '.' ~ col_name) | indent(indent) }}{{ "," if not loop.last }}
        {%- else %}
        {{ col_name | indent(indent) }}{{ "," if not loop.last }}
        {%- endif %}
    {%- endfor -%}

{%- endmacro -%}