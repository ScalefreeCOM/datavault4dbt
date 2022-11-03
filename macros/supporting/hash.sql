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

{%- set hash = var('datavault4dbt.hash', 'MD5') -%}
{%- set concat_string = var('concat_string', '||') -%}
{%- set quote = var('quote', '"') -%}
{%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}

{%- set hashkey_input_case_sensitive = var('datavault4dbt.hashkey_input_case_sensitive', FALSE) -%}
{%- set hashdiff_input_case_sensitive = var('datavault4dbt.hashdiff_input_case_sensitive', TRUE) -%}

{#- Select hashing algorithm -#}
{%- set hash_dtype = var('datavault4dbt.hash_datatype', 'STRING') -%}
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
    {%- set std_dict = fromjson(datavault4dbt.multi_active_concattenated_standardise(case_sensitive=hashdiff_input_case_sensitive, hash_alg=hash_alg, alias=alias, zero_key=unknown_key, multi_active_key=multi_active_key, main_hashkey_column=main_hashkey_column)) -%}
{%- elif is_hashdiff -%}
    {%- set std_dict = fromjson(datavault4dbt.concattenated_standardise(case_sensitive=hashdiff_input_case_sensitive, hash_alg=hash_alg, alias=alias, zero_key=unknown_key)) -%}
{%- else -%}
    {%- set std_dict = fromjson(datavault4dbt.concattenated_standardise(case_sensitive=hashkey_input_case_sensitive, hash_alg=hash_alg, alias=alias, zero_key=unknown_key)) -%}
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

        {{ standardise_suffix | indent(4) }}

    {%- else -%}

        {%- do all_null.append(concat_string) -%}

    {%- endif -%}

{%- endfor -%}

{%- endmacro -%}

{%- macro exasol__hash(columns, alias, is_hashdiff, multi_active_key, main_hashkey_column) -%}

    {%- set hash = var('datavault4dbt.hash', 'MD5') -%}
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
        {%- set std_dict = fromjson(datavault4dbt.multi_active_concattenated_standardise(case_sensitive=hashdiff_input_case_sensitive, hash_alg=hash_alg, alias=alias, zero_key=unknown_key, multi_active_key=multi_active_key, main_hashkey_column=main_hashkey_column)) -%}
    {%- elif is_hashdiff -%}
        {%- set std_dict = fromjson(datavault4dbt.concattenated_standardise(case_sensitive=hashdiff_input_case_sensitive, hash_alg=hash_alg, alias=alias, zero_key=unknown_key)) -%}
    {%- else -%}
        {%- set std_dict = fromjson(datavault4dbt.concattenated_standardise(case_sensitive=hashkey_input_case_sensitive, hash_alg=hash_alg, alias=none, zero_key=unknown_key)) -%}

        CASE WHEN COALESCE(
        {%- for column in columns -%}
            CAST({{ column }} AS VARCHAR(200000) UTF8) {%- if not loop.last -%} , {% endif -%}
        {% endfor -%}, NULL) IS NULL
        THEN CAST('{{ unknown_key }}' as {{ hash_dtype }})
        ELSE
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

            {{ standardise_suffix | indent(4) }}

        {%- else -%}

            {%- do all_null.append(concat_string) -%}

        {%- endif -%}

    {%- endfor -%}

    {% if not is_hashdiff -%}
    {{- "\n END " -}} {%- if alias is not none -%} {{" AS " }} "{{ alias }}" {%- endif -%}
    {%- endif -%}

{%- endmacro -%}
