{%- macro process_columns_to_select(columns_list=none, exclude_columns_list=none) -%}

    {{ return(adapter.dispatch('process_columns_to_select', 'datavault4dbt')(columns_list=columns_list,exclude_columns_list=exclude_columns_list)) }}

{%- endmacro -%}


{%- macro default__process_columns_to_select(columns_list, exclude_columns_list) -%}
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

    
{%- macro fabric__process_columns_to_select(columns_list, exclude_columns_list) -%}

    {% set set_casing = var('datavault4dbt.set_casing', none) %}
    {% if set_casing|lower in ['upper', 'uppercase'] %}
        {% set exclude_columns_list = exclude_columns_list | map('upper') | list %}
        {% set columns_list = columns_list | map('upper') | list %}
    {% elif set_casing|lower in ['lower', 'lowercase'] %}
        {% set exclude_columns_list = exclude_columns_list | map('lower') | list %}
        {% set columns_list = columns_list | map('lower') | list %}
    {% endif %}

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


{%- macro databricks__process_columns_to_select(columns_list, exclude_columns_list) -%}

    {% set set_casing = var('datavault4dbt.set_casing', none) %}
    {% if set_casing|lower in ['upper', 'uppercase'] %}
        {% set exclude_columns_list = exclude_columns_list | map('upper') | list %}
        {% set columns_list = columns_list | map('upper') | list %}
    {% elif set_casing|lower in ['lower', 'lowercase'] %}
        {% set exclude_columns_list = exclude_columns_list | map('lower') | list %}
        {% set columns_list = columns_list | map('lower') | list %}
    {% endif %}

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


{%- macro synapse__process_columns_to_select(columns_list, exclude_columns_list) -%}

    {{ return (datavault4dbt.default__process_columns_to_select(columns_list=columns_list,exclude_columns_list=exclude_columns_list)) }}

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

    {%- set ns = namespace(extracted_input_columns = []) -%}

    {%- if columns_dict is mapping -%}
        {%- for key, value in columns_dict.items() -%}
            {%- if value is mapping and 'src_cols_required' in value.keys() -%}
                {% if datavault4dbt.is_list(value['src_cols_required']) %}
                    {% set ns.extracted_input_columns = ns.extracted_input_columns + value['src_cols_required'] %}
                {% else %}
                    {%- do ns.extracted_input_columns.append(value['src_cols_required']) -%}
                {% endif %}
            {%- elif value is mapping and 'value' in value.keys() and 'src_cols_required' not in value.keys() -%}
                {# Do nothing. No source column required. #}    
            {%- elif value is mapping and value.is_hashdiff -%}
                {%- do ns.extracted_input_columns.append(value['columns']) -%}
            {%- else -%}
                {%- do ns.extracted_input_columns.append(value) -%}
            {%- endif -%}
        {%- endfor -%}
    
    {%- elif datavault4dbt.is_list(columns_dict) -%}
        {% for prejoin in columns_dict %}
            {%- if datavault4dbt.is_list(prejoin['this_column_name'])-%}
                {%- for column in prejoin['this_column_name'] -%}
                    {%- do ns.extracted_input_columns.append(column) -%}
                {%- endfor -%}
            {%- else -%}
                {%- do ns.extracted_input_columns.append(prejoin['this_column_name']) -%}
            {%- endif -%}
        {% endfor %}
    {%- else -%}
        {%- do return([]) -%}
    {%- endif -%}

    {%- do return(ns.extracted_input_columns) -%}

{%- endmacro -%}


{%- macro process_hash_column_excludes(hash_columns=none, source_columns=none) -%}

    {%- set processed_hash_columns = {} -%}

    {%- for col, col_mapping in hash_columns.items() -%}
        
        {%- if col_mapping is mapping -%}
            {%- if col_mapping.exclude_columns -%}

                {%- if col_mapping.columns -%}

                    {%- set columns_to_hash = datavault4dbt.process_columns_to_select(source_columns, col_mapping.columns) -%}

                    {%- do hash_columns[col].pop('exclude_columns') -%}
                    {%- do hash_columns[col].update({'columns': columns_to_hash}) -%}

                    {%- do processed_hash_columns.update({col: hash_columns[col]}) -%}
                {%- else -%}

                    {%- do hash_columns[col].pop('exclude_columns') -%}
                    {%- do hash_columns[col].update({'columns': source_columns}) -%}

                    {%- do processed_hash_columns.update({col: hash_columns[col]}) -%}
                {%- endif -%}
            {%- else -%}
                {%- do processed_hash_columns.update({col: col_mapping}) -%}
            {%- endif -%}
        {%- else -%}
            {%- do processed_hash_columns.update({col: col_mapping}) -%}
        {%- endif -%}

    {%- endfor -%}

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


{%- macro process_prejoined_columns(prejoined_columns=none) -%}
    {# Check if the old syntax is used for prejoined columns
        If so parse it to new list syntax #}

    {% if datavault4dbt.is_list(prejoined_columns) %}
        {% do return(prejoined_columns) %}
    {% else %}
        {% set output = [] %}

        {% for key, value in prejoined_columns.items() %}
            {% set ref_model = value.get('ref_model') %}
            {% set src_name = value.get('src_name') %}
            {% set src_table = value.get('src_table') %}
            {%- if 'operator' not in value.keys() -%}  
                {%- do value.update({'operator': 'AND'}) -%}
                {%- set operator = 'AND' -%}
            {%- else -%}
                {%- set operator = value.get('operator') -%}
            {%- endif -%}
            
    {% set match_criteria = (
            ref_model and output | selectattr('ref_model', 'equalto', ref_model) or
            src_name and output | selectattr('src_name', 'equalto', src_name) | selectattr('src_table', 'equalto', src_table)
        ) | selectattr('this_column_name', 'equalto', value.this_column_name)
        | selectattr('ref_column_name', 'equalto', value.ref_column_name)
        | selectattr('operator', 'equalto', value.operator)
        | list | first %}
        
            {% if match_criteria %}
                {% do match_criteria['extract_columns'].append(value.bk) %}
                {% do match_criteria['aliases'].append(key) %}
            {% else %}
                {% set new_item = {
                    'extract_columns': [value.bk],
                    'aliases': [key],
                    'this_column_name': value.this_column_name,
                    'ref_column_name': value.ref_column_name,
                    'operator': operator
                } %}
                
                {% if ref_model %}
                    {% do new_item.update({'ref_model': ref_model}) %}
                {% elif src_name and src_table %}
                    {% do new_item.update({'src_name': src_name, 'src_table': src_table}) %}
                {% endif %}
                
                {% do output.append(new_item) %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {%- do return(output) -%}

{%- endmacro -%}


{%- macro extract_prejoin_column_names(prejoined_columns=none) -%}

    {%- set extracted_column_names = [] -%}
    
    {% if not datavault4dbt.is_something(prejoined_columns) %}
        {%- do return(extracted_column_names) -%}
    {% endif %}

    {% for prejoin in prejoined_columns %}
        {% if datavault4dbt.is_list(prejoin['aliases']) %}
            {% for alias in prejoin['aliases'] %}
                {%- do extracted_column_names.append(alias) -%}
            {% endfor %}
        {% elif datavault4dbt.is_something(prejoin['aliases']) %}
            {%- do extracted_column_names.append(prejoin['aliases']) -%}
        {% elif datavault4dbt.is_list(prejoin['extract_columns']) %}
            {% for column in prejoin['extract_columns'] %}
                {%- do extracted_column_names.append(column) -%}
            {% endfor %}
        {% else %}
            {%- do extracted_column_names.append(prejoin['extract_columns']) -%}
        {% endif %}
    {%- endfor -%}
    
    {%- do return(extracted_column_names) -%}

{%- endmacro -%}
