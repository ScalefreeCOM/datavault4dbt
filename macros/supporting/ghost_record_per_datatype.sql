{%- macro ghost_record_per_datatype(column_name, datatype, ghost_record_type, col_size=none) -%}

{{ return(adapter.dispatch('ghost_record_per_datatype', 'datavault4dbt')(column_name=column_name,
                                                                            datatype=datatype,
                                                                            ghost_record_type=ghost_record_type,
                                                                            col_size=col_size)) }}
{%- endmacro -%}

{%- macro default__ghost_record_per_datatype(column_name, datatype, ghost_record_type, col_size) -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- set unknown_value__STRING = var('datavault4dbt.unknown_value__STRING', '(unknown)') -%}
{%- set error_value__STRING = var('datavault4dbt.error_value__STRING', '(error)') -%}
{%- if ghost_record_type == 'unknown' -%}
        {%- if datatype == 'TIMESTAMP' %} {{ datavault4dbt.string_to_timestamp( timestamp_format , beginning_of_all_times) }} as {{ column_name }}
        {%- elif datatype == 'STRING' %} '{{unknown_value__STRING}}' as {{ column_name }}
        {%- elif datatype == 'INT64' %} CAST('0' as INT64) as {{ column_name }}
        {%- elif datatype == 'FLOAT64' %} CAST('0' as FLOAT64) as {{ column_name }}
        {%- elif datatype == 'BOOLEAN' %} CAST('FALSE' as BOOLEAN) as {{ column_name }}
        {%- else %} CAST(NULL as {{ datatype }}) as {{ column_name }}
        {% endif %}
{%- elif ghost_record_type == 'error' -%}
        {%- if datatype == 'TIMESTAMP' %} {{ datavault4dbt.string_to_timestamp( timestamp_format , end_of_all_times) }} as {{ column_name }}
        {%- elif datatype == 'STRING' %} '{{error_value__STRING}}' as {{ column_name }}
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

{%- macro exasol__ghost_record_per_datatype(column_name, datatype, ghost_record_type, col_size) -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- set beginning_of_all_times_date = var('datavault4dbt.beginning_of_all_times_date', '0001-01-01') -%}
{%- set end_of_all_times_date = var('datavault4dbt.end_of_all_times_date', '8888-12-31') -%}


{%- set unknown_value__STRING = var('datavault4dbt.unknown_value__STRING', '(unknown)') -%}
{%- set error_value__STRING = var('datavault4dbt.error_value__STRING', '(error)') -%}
{%- set unknown_value_alt__STRING = var('datavault4dbt.unknown_value_alt__STRING', 'u')  -%}
{%- set error_value_alt__STRING = var('datavault4dbt.error_value_alt__STRING', 'e')  -%}
{%- set format_date = var('datavault4dbt.format_date', 'YYYY-mm-dd') -%}
{%- set hash = var('datavault4dbt.hash', 'MD5')-%}
{%- set hash_default_values =  datavault4dbt.hash_default_values(hash_function=hash) -%}
{%- set hash_alg= hash_default_values['hash_alg'] -%}
{%- set unknown_value__HASHTYPE = hash_default_values['unknown_key'] -%}
{%- set  error_value__HASHTYPE = hash_default_values['error_key'] -%}

{%- if ghost_record_type == 'unknown' -%}

        {%- if datatype == 'TIMESTAMP' or datatype == 'TIMESTAMP WITH LOCAL TIMEZONE' %} {{- datavault4dbt.string_to_timestamp( timestamp_format , beginning_of_all_times) }} as "{{ column_name }}"
        {%- elif datatype.upper().startswith('VARCHAR') -%}
            {%- if col_size is not none -%}
                {%- set unknown_dtype_length = col_size | int -%}
                {%- if '(' not in datatype -%}
                    {%- set datatype = datatype ~ "(" ~ (unknown_dtype_length|string) ~ ") UTF8" -%}
                {%- endif -%}
            {%- else -%}
                {%- set inside_parenthesis =  datatype.split(")")[0] |string -%}
                {%- set inside_parenthesis = inside_parenthesis.split("(")[1]-%}
                {%- set unknown_dtype_length = inside_parenthesis | int -%}
            {%- endif -%}
            {%- if unknown_dtype_length < unknown_value__STRING|length -%}
                CAST('{{ unknown_value_alt__STRING }}' as {{ datatype }} ) as "{{ column_name }}"
            {%- else -%}
                CAST('{{ unknown_value__STRING }}' as {{ datatype }} ) as "{{ column_name }}"
            {%- endif -%}
        {%- elif datatype.upper().startswith('CHAR') -%} CAST('{{ unknown_value_alt__STRING }}' as {{ datatype }}) as "{{ column_name }}"
        {%- elif datatype.upper().startswith('DECIMAL') -%} CAST('0' as {{ datatype }}) as "{{ column_name }}"
        {%- elif datatype == 'DOUBLE PRECISION' %} CAST('0' as DOUBLE PRECISION) as "{{ column_name }}"
        {%- elif datatype == 'DATE'-%} TO_DATE('{{ beginning_of_all_times_date }}', '{{ format_date }}' ) as "{{ column_name }}"
        {%- elif datatype == 'BOOLEAN' %} FALSE as "{{ column_name }}"
        {%- elif datatype.upper().startswith('HASHTYPE') -%} CAST('{{ unknown_value__HASHTYPE }}' as {{ datatype }}) as "{{ column_name }}"
        {%- else %} CAST(NULL as {{ datatype }}) as "{{ column_name }}"
        {% endif %}

{%- elif ghost_record_type == 'error' -%}

        {%- if datatype == 'TIMESTAMP' or datatype == 'TIMESTAMP WITH LOCAL TIME ZONE' %} {{- datavault4dbt.string_to_timestamp( timestamp_format , end_of_all_times) }} as "{{ column_name }}"
        {%- elif datatype.upper().startswith('VARCHAR') -%}
            {%- if col_size is not none -%}
                {%- set error_dtype_length = col_size | int -%}
                {%- if '(' not in datatype -%}
                    {%- set datatype = datatype ~ "(" ~ (error_dtype_length|string) ~ ") UTF8" -%}
                {%- endif -%}
            {%- else -%}
                {%- set inside_parenthesis =  datatype.split(")")[0] |string -%}
                {%- set inside_parenthesis = inside_parenthesis.split("(")[1]-%}
                {%- set error_dtype_length = inside_parenthesis | int -%}
            {%- endif -%}
            {%- if error_dtype_length < error_value__STRING|length  -%}
                CAST('{{ error_value_alt__STRING }}' as {{ datatype }} ) as "{{ column_name }}"
            {%- else -%}
                CAST('{{ error_value__STRING }}' as {{ datatype }} ) as "{{ column_name }}"
            {%- endif -%}
        {%- elif datatype.upper().startswith('CHAR') -%} CAST('{{ error_value_alt__STRING }}' as {{ datatype }}) as "{{ column_name }}"
        {%- elif datatype.upper().startswith('DECIMAL') -%} CAST('-1' as {{ datatype }}) as "{{ column_name }}"
        {%- elif datatype == 'DOUBLE PRECISION' %} CAST('-1' as DOUBLE PRECISION) as "{{ column_name }}"
        {%- elif datatype == 'BOOLEAN' %} FALSE as "{{ column_name }}"
        {%- elif datatype == 'DATE'-%} TO_DATE('{{ end_of_all_times_date }}', '{{ format_date }}' ) as "{{ column_name }}"
        {%- elif datatype.upper().startswith('HASHTYPE') -%} CAST('{{ error_value__HASHTYPE }}' as {{ datatype }}) as "{{ column_name }}"
        {%- else %} CAST(NULL as {{ datatype }}) as "{{ column_name }}"
        {% endif %}

{%- else -%}

    {%- if execute -%}
        {{ exceptions.raise_compiler_error("Invalid Ghost Record Type. Accepted are 'unknown' and 'error'.") }}
    {%- endif %}

{%- endif -%}

{%- endmacro -%}



{%- macro snowflake__ghost_record_per_datatype(column_name, datatype, ghost_record_type, col_size) -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- if ghost_record_type == 'unknown' -%}
     {%- if datatype in ['TIMESTAMP_NTZ','TIMESTAMP'] %}{{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }} AS {{ column_name }}
     {% elif datatype in ['STRING','VARCHAR'] %}'(unknown)' AS {{ column_name }}
     {% elif datatype in ['NUMBER','INT','FLOAT','DECIMAL'] %}0 AS {{ column_name }}
     {% elif datatype == 'BOOLEAN' %}CAST('FALSE' AS BOOLEAN) AS {{ column_name }}
     {% else %}NULL AS {{ column_name }}
     {% endif %}
{%- elif ghost_record_type == 'error' -%}
     {%- if datatype in ['TIMESTAMP_NTZ','TIMESTAMP'] %}{{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }} AS {{ column_name }}
     {% elif datatype in ['STRING','VARCHAR'] %}'(error)' AS {{ column_name }}
     {% elif datatype in ['NUMBER','INT','FLOAT','DECIMAL'] %}-1 AS {{ column_name }}
     {% elif datatype == 'BOOLEAN' %}CAST('FALSE' AS BOOLEAN) AS {{ column_name }}
     {% else %}NULL AS {{ column_name }}
      {% endif %}
{%- else -%}
    {%- if execute -%}
     {{ exceptions.raise_compiler_error("Invalid Ghost Record Type. Accepted are 'unknown' and 'error'.") }}
    {%- endif %}
{%- endif -%}

{%- endmacro -%}
