{%- macro ghost_record_per_datatype(column_name, datatype, ghost_record_type) -%}

{{ return(adapter.dispatch('ghost_record_per_datatype', 'dbtvault_scalefree')(column_name=column_name,
                                                                            datatype=datatype,
                                                                            ghost_record_type=ghost_record_type)) }}

{%- endmacro -%}                                                                            


{%- macro default__ghost_record_per_datatype(column_name, datatype, ghost_record_type) -%}

{%- set beginning_of_all_times = var('dbtvault_scalefree.beginning_of_all_times', '0001-01-01T00-00-01') -%}
{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}

{%- if ghost_record_type == 'unknown' -%}

        {%- if datatype == 'TIMESTAMP' %} {{ dbtvault_scalefree.string_to_timestamp( timestamp_format , beginning_of_all_times) }} as {{ column_name }}
        {%- elif datatype == 'STRING' %} '(unknown)' as {{ column_name }}
        {%- elif datatype == 'INT64' %} CAST('0' as INT64) as {{ column_name }}
        {%- elif datatype == 'FLOAT64' %} CAST('0' as FLOAT64) as {{ column_name }}
        {%- elif datatype == 'BOOLEAN' %} CAST('FALSE' as BOOLEAN) as {{ column_name }}
        {%- else %} CAST(NULL as {{ datatype }}) as {{ column_name }}
        {% endif %}

{%- elif ghost_record_type == 'error' -%}

        {%- if datatype == 'TIMESTAMP' %} {{ dbtvault_scalefree.string_to_timestamp( timestamp_format , end_of_all_times) }} as {{ column_name }}
        {%- elif datatype == 'STRING' %} '(error)' as {{ column_name }}
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
{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', 'YYYY-mm-ddTHH-MI-SS') -%}

{%- if ghost_record_type == 'unknown' -%}

        {%- if datatype == 'TIMESTAMP' %} {{ dbtvault_scalefree.string_to_timestamp( timestamp_format , beginning_of_all_times) }} as "{{ column_name }}"
        {%- elif datatype == 'VARCHAR' %} '(unknown)' as "{{ column_name }}"
        {%- elif datatype == 'DECIMAL' %} CAST('0' as DECIMAL) as {{ column_name }}
        {%- elif datatype == 'DOUBLE PRECISION' %} CAST('0' as DOUBLE PRECISION) as "{{ column_name }}"
        {%- elif datatype == 'BOOLEAN' %} CAST('FALSE' as BOOLEAN) as "{{ column_name }}"
        {%- else %} CAST(NULL as {{ datatype }}) as "{{ column_name }}"
        {% endif %}

{%- elif ghost_record_type == 'error' -%}

        {%- if datatype == 'TIMESTAMP' %} {{ dbtvault_scalefree.string_to_timestamp( timestamp_format , end_of_all_times) }} as "{{ column_name }}"
        {%- elif datatype == 'VARCHAR' %} '(error)' as "{{ column_name }}"
        {%- elif datatype == 'DECIMAL' %} CAST('-1' as DECIMAL) as "{{ column_name }}"
        {%- elif datatype == 'DOUBLE PRECISION' %} CAST('-1' as DOUBLE PRECISION) as "{{ column_name }}"
        {%- elif datatype == 'BOOLEAN' %} CAST('FALSE' as BOOLEAN) as "{{ column_name }}"
        {%- else %} CAST(NULL as {{ datatype }}) as "{{ column_name }}"
        {% endif %}

{%- else -%}

    {%- if execute -%}
        {{ exceptions.raise_compiler_error("Invalid Ghost Record Type. Accepted are 'unknown' and 'error'.") }}
    {%- endif %}

{%- endif -%}

{%- endmacro -%}