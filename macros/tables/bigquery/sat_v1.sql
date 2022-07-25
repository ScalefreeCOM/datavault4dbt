
{%- macro sat_v1(source_sat, src_hk, src_hd, src_ldts='ldts', ledts_alias='ledts') -%}

    {{ adapter.dispatch('sat_v1', 'dbtvault_scalefree')(source_sat=source_sat,
                                         src_hk=src_hk,
                                         src_hd=src_hd,
                                         src_ldts=src_ldts,
                                         ledts_alias=ledts_alias) }}

{%- endmacro -%}

{%- macro default__sat_v1(source_sat, src_hk, src_hd, src_ldts, ledts_alias) -%}
    {%- set all_columns = adapter.get_columns_in_relation(ref(source_sat)) -%}
    {%- set exclude = [src_hk, src_hd, src_ldts] -%}

    {%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
    {%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}

    {%- set hash = var('dbtvault_scalefree.hash', 'MD5') -%}
    {%- set hash_alg, unknown_key, error_key = dbtvault_scalefree.hash_default_values(hash_function=hash) -%}

    {{ prepend_generated_by() }}

    SELECT
        {{ src_hk }},
        {{ src_ldts }},
        COALESCE(LEAD(TIMESTAMP_SUB(ldts, INTERVAL 1 MICROSECOND)) OVER (PARTITION BY {{ src_hk }} ORDER BY {{ src_ldts }}),PARSE_TIMESTAMP('{{ timestamp_format }}', '{{ end_of_all_times }}')) as {{ ledts_alias }},
        {{ src_hd }},
        {%- for column in all_columns -%}
            {%- if column.name not in exclude -%}
                {{ column.name }}
                {{ "," if not loop.last }}
            {%- endif -%}
        {%- endfor -%}
    FROM {{ ref(source_sat) }}
    WHERE {{ src_hd }} != '{{ error_key }}'

{%- endmacro -%}