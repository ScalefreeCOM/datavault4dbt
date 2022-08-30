{%- macro ghost_record_per_datatype(column_name, datatype, ghost_record_type) -%}
{{ return(adapter.dispatch('ghost_record_per_datatype', 'dbtvault_scalefree')(column_name=column_name,
                                                                            datatype=datatype,
                                                                            ghost_record_type=ghost_record_type)) }}
{%- endmacro -%}
{%- macro default__ghost_record_per_datatype(column_name, datatype, ghost_record_type) -%}
{%- set beginning_of_all_times = var('dbtvault_scalefree.beginning_of_all_times', '0001-01-01T00-00-01') -%}
{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}
{%- set unknown_value__STRING_ghost_record = var('dbtvault_scalefree.unknown_value__STRING_ghost_record', '(unknown)') -%}
{%- set error_value__STRING_ghost_record = var('dbtvault_scalefree.error_value__STRING_ghost_record', '(error)') -%}
{%- if ghost_record_type == 'unknown' -%}
        {%- if datatype == 'TIMESTAMP' %} {{ dbtvault_scalefree.string_to_timestamp( timestamp_format , beginning_of_all_times) }} as {{ column_name }}
        {%- elif datatype == 'STRING' %} '{{unknown_value__STRING_ghost_record}}' as {{ column_name }}
        {%- elif datatype == 'INT64' %} CAST('0' as INT64) as {{ column_name }}
        {%- elif datatype == 'FLOAT64' %} CAST('0' as FLOAT64) as {{ column_name }}
        {%- elif datatype == 'BOOLEAN' %} CAST('FALSE' as BOOLEAN) as {{ column_name }}
        {%- else %} CAST(NULL as {{ datatype }}) as {{ column_name }}
        {% endif %}
{%- elif ghost_record_type == 'error' -%}
        {%- if datatype == 'TIMESTAMP' %} {{ dbtvault_scalefree.string_to_timestamp( timestamp_format , end_of_all_times) }} as {{ column_name }}
        {%- elif datatype == 'STRING' %} '{{error_value__STRING_ghost_record}}' as {{ column_name }}
        {%- elif datatype == 'INT64' %} CAST('-1' as INT64) as {{ column_name }}
        {%- elif datatype == 'FLOAT64' %} CAST('-1' as FLOAT64) as {{ column_name }}
        {%- elif datatype == 'BOOLEAN' %} CAST('FALSE' as BOOLEAN) as {{ column_name }}
        {%- else %} CAST(NULL as {{ datatype }}) as {{ column_name }}
        {% endif %}
{%- else -%}
    {%- if execute -%}
        {{ exceptions.raise_compiler_error("Invalid Ghost Record Type. Accepted are 'unknown' and 'error'.") }}
    {%- endif %}
{%- endif -%}
{%- endmacro -%}

{%- macro exasol__ghost_record_per_datatype(column_name, datatype, ghost_record_type) -%}

{%- set beginning_of_all_times = var('dbtvault_scalefree.beginning_of_all_times', '0001-01-01T00-00-01') -%}
{%- set beginning_of_all_times_date = var('dbtvault_scalefree.beginning_of_all_times_date', '0001-01-01') -%}
{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set end_of_all_times_date = var('dbtvault_scalefree.end_of_all_times_date', '8888-12-31') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', 'YYYY-mm-ddTHH-MI-SS') -%}
{%- set unknown_value__VARCHAR_ghost_record = var('dbtvault_scalefree.unknown_value__VARCHAR_ghost_record', '(unknown)') -%}
{%- set error_value__VARCHAR_ghost_record = var('dbtvault_scalefree.error_value__VARCHAR_ghost_record', '(error)') -%}
{%- set unknown_value_alt__VARCHAR_ghost_record = var('dbtvault_scalefree.unknown_value_alt__VARCHAR_ghost_record', 'u')  -%}
{%- set error_value_alt__VARCHAR_ghost_record = var('dbtvault_scalefree.error_value_alt__VARCHAR_ghost_record', 'e')  -%}
{%- set format_date = var('dbtvault_scalefree.format_date', 'YYYY-mm-dd') -%}
{%- set hash = var('dbtvault_scalefree.hash', 'MD5')-%}
{%- set hash_alg, unknown_value__HASHTYPE_ghost_record, error_value__HASHTYPE_ghost_record = dbtvault_scalefree.hash_default_values(hash_function=hash) -%}

{%- if ghost_record_type == 'unknown' -%}

        {%- if datatype == 'TIMESTAMP' or datatype == 'TIMESTAMP WITH LOCAL TIMEZONE' %} {{ dbtvault_scalefree.string_to_timestamp( timestamp_format , beginning_of_all_times) }} as "{{ column_name }}"
        {%- elif datatype == 'VARCHAR' -%} CAST('{{ unknown_value_alt__VARCHAR_ghost_record }}' as VARCHAR(2000000) UTF8) as "{{ column_name }}"
        {%- elif datatype.upper().startswith('VARCHAR') -%}
            {%- set unknown_dtype_length = datatype.split(")")[0].split("(")[1] | int -%}
            {%- if unknown_dtype_length < unknown_value__VARCHAR_ghost_record|length -%}
                CAST('{{ unknown_value_alt__VARCHAR_ghost_record }}' as {{ datatype }} ) as "{{ column_name }}"
            {%- else -%}
                CAST('{{ unknown_value__VARCHAR_ghost_record }}' as {{ datatype }} ) as "{{ column_name }}"
            {%- endif -%}
        {%- elif datatype.upper().startswith('CHAR') -%} CAST('{{ unknown_value_alt__VARCHAR_ghost_record }}' as {{ datatype }}) as "{{ column_name }}"
        {%- elif datatype.upper().startswith('DECIMAL') -%} CAST('0' as {{ datatype }}) as "{{ column_name }}"
        {%- elif datatype == 'DOUBLE PRECISION' %} CAST('0' as DOUBLE PRECISION) as "{{ column_name }}"
        {%- elif datatype == 'DATE'-%} TO_DATE('{{ beginning_of_all_times_date }}', '{{ format_date }}' ) as "{{ column_name }}"
        {%- elif datatype == 'BOOLEAN' %} FALSE as "{{ column_name }}"
        {%- elif datatype.upper().startswith('HASHTYPE') -%} CAST('{{ unknown_value__HASHTYPE_ghost_record }}' as {{ datatype }}) as "{{ column_name }}"
        {%- else %} CAST(NULL as {{ datatype }}) as "{{ column_name }}"
        {% endif %}

{%- elif ghost_record_type == 'error' -%}

        {%- if datatype == 'TIMESTAMP' or datatype == 'TIMESTAMP WITH LOCAL TIME ZONE' %} {{ dbtvault_scalefree.string_to_timestamp( timestamp_format , end_of_all_times) }} as "{{ column_name }}"
        {%- elif datatype == 'VARCHAR' -%} CAST('{{ error_value_alt__VARCHAR_ghost_record }}' as VARCHAR(2000000) UTF8) as "{{ column_name }}"
        {%- elif datatype.upper().startswith('VARCHAR') -%}
            {%- set error_dtype_length = datatype.split(")")[0].split("(")[1] | int -%}
            {%- if error_dtype_length < error_value__VARCHAR_ghost_record|length  -%}
                CAST('{{ error_value_alt__VARCHAR_ghost_record }}' as {{ datatype }} ) as "{{ column_name }}"
            {%- else -%}
                CAST('{{ error_value__VARCHAR_ghost_record }}' as {{ datatype }} ) as "{{ column_name }}"
            {%- endif -%}
        {%- elif datatype.upper().startswith('CHAR') -%} CAST('{{ error_value_alt__VARCHAR_ghost_record }}' as {{ datatype }}) as "{{ column_name }}"
        {%- elif datatype.upper().startswith('DECIMAL') -%} CAST('-1' as {{datatype}}) as "{{ column_name }}"
        {%- elif datatype == 'DOUBLE PRECISION' %} CAST('-1' as DOUBLE PRECISION) as "{{ column_name }}"
        {%- elif datatype == 'BOOLEAN' %} FALSE as "{{ column_name }}"
        {%- elif datatype == 'DATE'-%} TO_DATE('{{ end_of_all_times_date }}', '{{ format_date }}' ) as "{{ column_name }}"
        {%- elif datatype.upper().startswith('HASHTYPE') -%} CAST('{{ error_value__HASHTYPE_ghost_record }}' as {{ datatype }}) as "{{ column_name }}"
        {%- else %} CAST(NULL as {{ datatype }}) as "{{ column_name }}"
        {% endif %}

{%- else -%}

    {%- if execute -%}
        {{ exceptions.raise_compiler_error("Invalid Ghost Record Type. Accepted are 'unknown' and 'error'.") }}
    {%- endif %}

{%- endif -%}

{%- endmacro -%}
