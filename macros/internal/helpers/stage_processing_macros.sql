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
{# Check if the new list syntax is used for prejoined columns
    If so parse it to dictionaries #}

{% if not datavault4dbt.is_list(prejoined_columns) %}
    {% do return(prejoined_columns) %}
{% else %}
    {# if the (new) list syntax for prejoins is used
    it needs to be converted to the old syntax #}

    {# Initialize emtpy dict which will be filled by each entry #}
    {% set return_dict = {} %}

    {# Iterate over each dictionary in the prejoined_colums-list #}
    {% for dict_item in prejoined_columns %}

        {# If column aliases are present they they have to map 1:1 to the extract_columns #}
        {% if datavault4dbt.is_something(dict_item.aliases) 
            and not dict_item.aliases|length ==  dict_item.extract_columns|length %}
            {{ exceptions.raise_compiler_error("Prejoin aliases must have the same length as extract_columns") }}
        {% endif %}

        {# If multiple columns from the same source should be extracted each column has to be processed once #}
        {% if datavault4dbt.is_list(dict_item.extract_columns) %}
            {% for column in dict_item.extract_columns %}
                {# If aliases are defined they should be used as dict keys
                These will be used as new column names #}
                {% if datavault4dbt.is_something(dict_item.aliases) %}
                    {% set dict_key = dict_item.aliases[loop.index0] %}
                {% else %}
                    {% set dict_key = dict_item.extract_columns[loop.index0] %}
                {% endif %}

                {# To make sure each column or alias is present only once #}
                {% if dict_key|lower in return_dict.keys()|map('lower') %}
                    {{ exceptions.raise_compiler_error("Prejoined Column name or alias '" ~ dict_key ~ "' is defined twice.") }}
                {% endif %}

                {% set tmp_dict %}
                {{dict_key}}:
                    ref_model: {{dict_item.ref_model}}
                    bk: {{dict_item.extract_columns[loop.index0]}}
                    this_column_name: {{dict_item.this_column_name}}
                    ref_column_name: {{dict_item.ref_column_name}}
                {% endset %}
                {% do return_dict.update(fromyaml(tmp_dict)) %}
            {% endfor %}

        {% else %}

            {# If aliases are defined they should be used as dict keys
            These will be used as new column names #}
            {% if datavault4dbt.is_something(dict_item.aliases) %}
                {% set dict_key = dict_item.aliases[loop.index0] %}
            {% else %}
                {% set dict_key = dict_item.extract_columns[loop.index0] %}
            {% endif %}

            {# To make sure each column or alias is present only once #}
            {% if dict_key|lower in return_dict.keys()|map('lower') %}
                {{ exceptions.raise_compiler_error("Prejoined Column name or alias '" ~ dict_key ~ "' is defined twice.") }}
            {% endif %}

            {% set tmp_dict %}
            {{dict_key}}:
                ref_model: {{dict_item.ref_model}}
                bk: {{dict_item.extract_columns[loop.index0]}}
                this_column_name: {{dict_item.this_column_name}}
                ref_column_name: {{dict_item.ref_column_name}}
            {% endset %}
            {% do return_dict.update(fromyaml(tmp_dict)) %}
        {% endif %}
    {% endfor %}

    {%- do return(return_dict) -%}

{% endif %}

{%- endmacro -%}
