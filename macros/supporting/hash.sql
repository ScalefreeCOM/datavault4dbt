{%- macro hash(columns=none, alias=none, is_hashdiff=false, multi_active_key=none, main_hashkey_column=none) -%}

    {%- if is_hashdiff is none -%}
        {%- set is_hashdiff = false -%}
    {%- endif -%}

    {{- adapter.dispatch('hash', 'datavault4dbt')(columns=columns,
                                             alias=alias,
                                             is_hashdiff=is_hashdiff,
                                             multi_active_key=multi_active_key,
                                             main_hashkey_column=main_hashkey_column) -}}

{%- endmacro %}


{%- macro default__hash(columns, alias, is_hashdiff, multi_active_key, main_hashkey_column) -%}

{%- set hash = datavault4dbt.hash_method() -%}
{%- set concat_string = var('concat_string', '||') -%}
{%- set quote = var('quote', '"') -%}
{%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}

{%- set hashkey_input_case_sensitive = var('datavault4dbt.hashkey_input_case_sensitive', FALSE) -%}
{%- set hashdiff_input_case_sensitive = var('datavault4dbt.hashdiff_input_case_sensitive', TRUE) -%}

{#- Select hashing algorithm -#}
{%- set hash_dtype = var('datavault4dbt.hash_datatype', 'STRING') -%}
{{ log('hash type in hash macro: ' ~ hash_dtype, false) }}
{%- set hash_default_values = fromjson(datavault4dbt.hash_default_values(hash_function=hash,hash_datatype=hash_dtype)) -%}
{%- set hash_alg = hash_default_values['hash_alg'] -%}
{%- set unknown_key = hash_default_values['unknown_key'] -%}
{%- set error_key = hash_default_values['error_key'] -%}

{%- set attribute_standardise = datavault4dbt.attribute_standardise() %}


{#- If single column to hash -#}
{%- if columns is string -%}
    {%- set columns = [columns] -%}
{%- endif -%}

{%- set all_null = [] -%}

{%- if is_hashdiff  and datavault4dbt.is_something(multi_active_key) -%}
    {%- set std_dict = fromjson(datavault4dbt.multi_active_concattenated_standardise(case_sensitive=hashdiff_input_case_sensitive, hash_alg=hash_alg, datatype=hash_dtype, alias=alias, zero_key=unknown_key, multi_active_key=multi_active_key, main_hashkey_column=main_hashkey_column)) -%}
{%- elif is_hashdiff -%}
    {%- set std_dict = fromjson(datavault4dbt.concattenated_standardise(case_sensitive=hashdiff_input_case_sensitive, hash_alg=hash_alg, datatype=hash_dtype, alias=alias, zero_key=unknown_key)) -%}
{%- else -%}
    {%- set std_dict = fromjson(datavault4dbt.concattenated_standardise(case_sensitive=hashkey_input_case_sensitive, hash_alg=hash_alg, datatype=hash_dtype, alias=alias, zero_key=unknown_key)) -%}
{%- endif -%}

    {%- set standardise_prefix = std_dict['standardise_prefix'] -%}
    {%- set standardise_suffix = std_dict['standardise_suffix'] -%}

{{ standardise_prefix }}

{%- for column in columns -%}

    {%- do all_null.append(null_placeholder_string) -%}

    {%- if '.' in column %}
        {% set column_str = column -%}
    {%- else -%}
        {%- set column_str = datavault4dbt.as_constant(column) -%}
    {%- endif -%}

    {{- "\nIFNULL(({}), '{}')".format(attribute_standardise | replace('[EXPRESSION]', column_str) | replace('[QUOTE]', quote) | replace('[NULL_PLACEHOLDER_STRING]', null_placeholder_string), null_placeholder_string) | indent(4) -}}
    {{- ",'{}',".format(concat_string) if not loop.last -}}

    {%- if loop.last -%}

        {{ standardise_suffix | replace('[ALL_NULL]', all_null | join("")) | indent(4) }}

    {%- else -%}

        {%- do all_null.append(concat_string) -%}

    {%- endif -%}

{%- endfor -%}

{%- endmacro -%}

{%- macro exasol__hash(columns, alias, is_hashdiff, multi_active_key, main_hashkey_column) -%}

    {%- set hash = datavault4dbt.hash_method() -%}
    {%- set concat_string = var('concat_string', '||') -%}
    {%- set quote = var('quote', '"') -%}
    {%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}

    {%- set hashkey_input_case_sensitive = var('datavault4dbt.hashkey_input_case_sensitive', FALSE) -%}
    {%- set hashdiff_input_case_sensitive = var('datavault4dbt.hashdiff_input_case_sensitive', TRUE) -%}

    {#- Select hashing algorithm -#}
    {%- set hash_dtype = var('datavault4dbt.hash_datatype', 'HASHTYPE') -%}
    {%- set hash_default_values = fromjson(datavault4dbt.hash_default_values(hash_function=hash,hash_datatype=hash_dtype)) -%}
    {%- set hash_alg = hash_default_values['hash_alg'] -%}
    {%- set unknown_key = hash_default_values['unknown_key'] -%}
    {%- set error_key = hash_default_values['error_key'] -%}

    {%- set attribute_standardise = datavault4dbt.attribute_standardise() %}

    {#- If single column to hash -#}
    {%- if columns is string -%}
        {%- set columns = [columns] -%}
    {%- endif -%}

    {%- set all_null = [] -%}

    {%- if is_hashdiff  and datavault4dbt.is_something(multi_active_key) -%}
        {%- set std_dict = fromjson(datavault4dbt.multi_active_concattenated_standardise(case_sensitive=hashdiff_input_case_sensitive, hash_alg=hash_alg, datatype=hash_dtype, alias=alias, zero_key=unknown_key, multi_active_key=multi_active_key, main_hashkey_column=main_hashkey_column)) -%}
    {%- elif is_hashdiff -%}
        {%- set std_dict = fromjson(datavault4dbt.concattenated_standardise(case_sensitive=hashdiff_input_case_sensitive, hash_alg=hash_alg, datatype=hash_dtype, alias=alias, zero_key=unknown_key)) -%}
    {%- else -%}
        {%- set std_dict = fromjson(datavault4dbt.concattenated_standardise(case_sensitive=hashkey_input_case_sensitive, hash_alg=hash_alg, datatype=hash_dtype, alias=alias, zero_key=unknown_key)) -%}

    {%- endif -%}

    {%- set standardise_prefix = std_dict['standardise_prefix'] -%}
    {%- set standardise_suffix = std_dict['standardise_suffix'] -%}

    {{" "~ standardise_prefix }}

    {%- for column in columns -%}

        {%- do all_null.append(null_placeholder_string) -%}

        {%- if '.' in column %}
            {% set column_str = column -%}
        {%- else -%}
            {%- set column_str = datavault4dbt.as_constant(column) -%}
        {%- endif -%}

        {{- "\n NULLIF(({}), '{}')".format(attribute_standardise | replace('[EXPRESSION]', column_str) | replace('[QUOTE]', quote) | replace('[NULL_PLACEHOLDER_STRING]', null_placeholder_string), null_placeholder_string) | indent(4) -}}
        {{- ",'{}',".format(concat_string) if not loop.last -}}

        {%- if loop.last -%}

            {{ standardise_suffix | replace('[ALL_NULL]', all_null | join("")) | indent(4) }}

        {%- else -%}

            {%- do all_null.append(concat_string) -%}

        {%- endif -%}

    {%- endfor -%}

{%- endmacro -%}


{%- macro synapse__hash(columns, alias, is_hashdiff, multi_active_key, main_hashkey_column) -%}

{%- set hash = var('datavault4dbt.hash', 'MD5') -%}
{%- set concat_string = var('concat_string', '||') -%}
{%- set quote = var('quote', '"') -%}
{%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}

{%- set hashkey_input_case_sensitive = var('datavault4dbt.hashkey_input_case_sensitive', FALSE) -%}
{%- set hashdiff_input_case_sensitive = var('datavault4dbt.hashdiff_input_case_sensitive', TRUE) -%}

{#- Select hashing algorithm -#}
{%- set hash_dtype = var('datavault4dbt.hash_datatype', 'BINARY(16)') -%}
{{ log('hash type in hash macro: ' ~ hash_dtype, false) }}
{%- set hash_default_values = fromjson(datavault4dbt.hash_default_values(hash_function=hash,hash_datatype=hash_dtype)) -%}
{%- set hash_alg = hash_default_values['hash_alg'] -%}
{%- set unknown_key = hash_default_values['unknown_key'] -%}
{%- set error_key = hash_default_values['error_key'] -%}

{%- if is_hashdiff -%}
    {%- set attribute_standardise = datavault4dbt.attribute_standardise(hash_type='hashdiff') %}
{%- else -%}
    {%- set attribute_standardise = datavault4dbt.attribute_standardise(hash_type='hashkey') %}
{%- endif -%}

{#- If single column to hash -#}
{%- if columns is string -%}
    {%- set columns = [columns] -%}
{%- endif -%}

{%- set all_null = [] -%}

{%- if is_hashdiff  and datavault4dbt.is_something(multi_active_key) -%}
    {%- set std_dict = fromjson(datavault4dbt.multi_active_concattenated_standardise(case_sensitive=hashdiff_input_case_sensitive, hash_alg=hash_alg, datatype=hash_dtype, alias=alias, zero_key=unknown_key, multi_active_key=multi_active_key, main_hashkey_column=main_hashkey_column)) -%}
{%- elif is_hashdiff -%}
    {%- set std_dict = fromjson(datavault4dbt.concattenated_standardise(case_sensitive=hashdiff_input_case_sensitive, hash_alg=hash_alg, datatype=hash_dtype, alias=alias, zero_key=unknown_key)) -%}
{%- else -%}
    {%- set std_dict = fromjson(datavault4dbt.concattenated_standardise(case_sensitive=hashkey_input_case_sensitive, hash_alg=hash_alg, datatype=hash_dtype, alias=alias, zero_key=unknown_key)) -%}
{%- endif -%}

    {%- set standardise_prefix = std_dict['standardise_prefix'] -%}
    {%- set standardise_suffix = std_dict['standardise_suffix'] -%}

{{ standardise_prefix }}

{%- for column in columns -%}

    {%- do all_null.append(null_placeholder_string) -%}

    {%- if '.' in column %}
        {% set column_str = column -%}
    {%- else -%}
        {%- set column_str = datavault4dbt.as_constant(column) -%}
    {%- endif -%}
    {{ log('attribute_standardise: '~attribute_standardise, false)}}

    {{- "\nISNULL(({}), '{}')".format(attribute_standardise | replace('[EXPRESSION]', column_str) | replace('[QUOTE]', quote) | replace('[NULL_PLACEHOLDER_STRING]', null_placeholder_string), null_placeholder_string) | indent(4) -}}
    {{- ",'{}',".format(concat_string) if not loop.last -}}
    {{- ", ''" if columns|length == 1 -}}

    {%- if loop.last -%}

        {{ standardise_suffix | replace('[ALL_NULL]', all_null | join("")) | indent(4) }}

    {%- else -%}

        {%- do all_null.append(concat_string) -%}

    {%- endif -%}

{%- endfor -%}

{%- endmacro -%}    


{%- macro postgres__hash(columns, alias, is_hashdiff, multi_active_key, main_hashkey_column, rtrim_hashdiff) -%}


{%- set hash = var('datavault4dbt.hash', 'MD5') -%}
{%- set concat_string = var('concat_string', '||') -%}
{%- set quote = var('quote', '"') -%}
{%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}

{%- set hashkey_input_case_sensitive = var('datavault4dbt.hashkey_input_case_sensitive', FALSE) -%}
{%- set hashdiff_input_case_sensitive = var('datavault4dbt.hashdiff_input_case_sensitive', TRUE) -%}

{#- Select hashing algorithm -#}

{%- set hash_dtype = var('datavault4dbt.hash_datatype', 'VARCHAR') -%}

{{ log('hash type in hash macro: ' ~ hash_dtype, false) }}
{%- set hash_default_values = fromjson(datavault4dbt.hash_default_values(hash_function=hash,hash_datatype=hash_dtype)) -%}
{%- set hash_alg = hash_default_values['hash_alg'] -%}
{%- set unknown_key = hash_default_values['unknown_key'] -%}
{%- set error_key = hash_default_values['error_key'] -%}

{%- set attribute_standardise = datavault4dbt.attribute_standardise() %}


{#- If single column to hash -#}
{%- if columns is string -%}
    {%- set columns = [columns] -%}
{%- endif -%}

{%- set all_null = [] -%}

{%- if is_hashdiff  and datavault4dbt.is_something(multi_active_key) -%}
    {%- set std_dict = fromjson(datavault4dbt.multi_active_concattenated_standardise(case_sensitive=hashdiff_input_case_sensitive, hash_alg=hash_alg, datatype=hash_dtype, alias=alias, zero_key=unknown_key, multi_active_key=multi_active_key, main_hashkey_column=main_hashkey_column)) -%}
{%- elif is_hashdiff -%}
    {%- set std_dict = fromjson(datavault4dbt.concattenated_standardise(case_sensitive=hashdiff_input_case_sensitive, hash_alg=hash_alg, datatype=hash_dtype, alias=alias, zero_key=unknown_key,is_hashdiff=is_hashdiff, rtrim_hashdiff=rtrim_hashdiff)) -%}
{%- else -%}
    {%- set std_dict = fromjson(datavault4dbt.concattenated_standardise(case_sensitive=hashkey_input_case_sensitive, hash_alg=hash_alg, datatype=hash_dtype, alias=alias, zero_key=unknown_key,is_hashdiff=is_hashdiff, rtrim_hashdiff=rtrim_hashdiff)) -%}
{%- endif -%}

    {%- set standardise_prefix = std_dict['standardise_prefix'] -%}
    {%- set standardise_suffix = std_dict['standardise_suffix'] -%}

{{ standardise_prefix }}

{%- for column in columns -%}

    {%- do all_null.append(null_placeholder_string) -%}

    {%- if '.' in column %}
        {% set column_str = column -%}
    {%- else -%}
        {%- set column_str = datavault4dbt.as_constant(column) -%}
    {%- endif -%}

    {{- "\nCOALESCE(({}), '{}')".format(attribute_standardise | replace('[EXPRESSION]', column_str) | replace('[QUOTE]', quote) | replace('[NULL_PLACEHOLDER_STRING]', null_placeholder_string), null_placeholder_string) | indent(4) -}}
    {{- ",'{}',".format(concat_string) if not loop.last -}}

    {%- if loop.last -%}

        {{ standardise_suffix | replace('[ALL_NULL]', all_null | join("")) | indent(4) }}

    {%- else -%}

        {%- do all_null.append(concat_string) -%}

    {%- endif -%}

{%- endfor -%}

{%- endmacro -%}


{%- macro redshift__hash(columns, alias, is_hashdiff, multi_active_key, main_hashkey_column, rtrim_hashdiff) -%}

{%- set hash = var('datavault4dbt.hash', 'MD5') -%}
{%- set concat_string = var('concat_string', '|') -%}
{%- set quote = var('quote', '"') -%}
{%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}

{%- set hashkey_input_case_sensitive = var('datavault4dbt.hashkey_input_case_sensitive', FALSE) -%}
{%- set hashdiff_input_case_sensitive = var('datavault4dbt.hashdiff_input_case_sensitive', TRUE) -%}

{#- Select hashing algorithm -#}
{%- set hash_dtype = var('datavault4dbt.hash_datatype', 'VARCHAR') -%}
{{ log('hash type in hash macro: ' ~ hash_dtype, false) }}
{%- set hash_default_values = fromjson(datavault4dbt.hash_default_values(hash_function=hash,hash_datatype=hash_dtype)) -%}
{%- set hash_alg = hash_default_values['hash_alg'] -%}
{%- set unknown_key = hash_default_values['unknown_key'] -%}
{%- set error_key = hash_default_values['error_key'] -%}

{%- set attribute_standardise = datavault4dbt.attribute_standardise() %}

{#- If single column to hash -#}
{%- if columns is string -%}
    {%- set columns = [columns] -%}
{%- endif -%}

{%- set all_null = [] -%}

{%- if is_hashdiff  and datavault4dbt.is_something(multi_active_key) -%}
    {%- set std_dict = fromjson(datavault4dbt.multi_active_concattenated_standardise(case_sensitive=hashdiff_input_case_sensitive, hash_alg=hash_alg, datatype=hash_dtype, alias=alias, zero_key=unknown_key, multi_active_key=multi_active_key, main_hashkey_column=main_hashkey_column)) -%}
{%- elif is_hashdiff -%}
    {%- set std_dict = fromjson(datavault4dbt.concattenated_standardise(case_sensitive=hashdiff_input_case_sensitive, hash_alg=hash_alg, datatype=hash_dtype, alias=alias, zero_key=unknown_key,is_hashdiff=is_hashdiff, rtrim_hashdiff=rtrim_hashdiff)) -%}
{%- else -%}
    {%- set std_dict = fromjson(datavault4dbt.concattenated_standardise(case_sensitive=hashkey_input_case_sensitive, hash_alg=hash_alg, datatype=hash_dtype, alias=alias, zero_key=unknown_key,is_hashdiff=is_hashdiff, rtrim_hashdiff=rtrim_hashdiff)) -%}
{%- endif -%}

    {%- set standardise_prefix = std_dict['standardise_prefix'] -%}
    {%- set standardise_suffix = std_dict['standardise_suffix'] -%}

{{ standardise_prefix }}

{%- for column in columns -%}

    {%- do all_null.append(null_placeholder_string) -%}

    {%- if '.' in column %}
        {% set column_str = column -%}
    {%- else -%}
        {%- set column_str = datavault4dbt.as_constant(column) -%}
    {%- endif -%}

    {{- "\nCOALESCE({}, '{}')".format(attribute_standardise | replace('[EXPRESSION]', column_str) | replace('[QUOTE]', quote) | replace('[NULL_PLACEHOLDER_STRING]', null_placeholder_string), null_placeholder_string) | indent(4) -}}
    {{- "|| '{}' ||".format(concat_string) if not loop.last -}}


    {%- if loop.last -%}

        {{ standardise_suffix | replace('[ALL_NULL]', all_null | join("")) | indent(4) }}

    {%- else -%}

        {%- do all_null.append(concat_string) -%}

    {%- endif -%}

{%- endfor -%}

{%- endmacro -%}