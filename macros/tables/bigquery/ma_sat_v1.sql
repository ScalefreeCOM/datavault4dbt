
{%- macro ma_sat_v1(source_sat, src_hk, src_hd, src_ma, src_ldts='ldts', ledts_alias='ledts') -%}

    {{ adapter.dispatch('ma_sat_v1', 'dbtvault_scalefree')(source_sat=source_sat,
                                         src_hk=src_hk,
                                         src_hd=src_hd,
                                         src_ma=src_ma,
                                         src_ldts=src_ldts,
                                         ledts_alias=ledts_alias) }}

{%- endmacro -%}

{%- macro default__ma_sat_v1(source_sat, src_hk, src_hd, src_ma, src_ldts, ledts_alias) -%}
    {%- set all_columns = adapter.get_columns_in_relation(ref(source_sat)) -%}
    {%- set exclude = [src_hk, src_hd, src_ma, src_ldts] -%}

    {%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
    {%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}

    {%- set hash = var('dbtvault_scalefree.hash', 'MD5') -%}
    {%- set hash_alg, unknown_key, error_key = dbtvault_scalefree.hash_default_values(hash_function=hash) -%}

    {{ prepend_generated_by() }}

    WITH 
    mas_data AS (
        SELECT * 
        FROM {{ ref(source_sat) }}
    ), 

    ord_ldts AS (
        SELECT DISTINCT 
            {{ src_hashdiff }}
        , {{ src_ldts }}
        FROM mas_data
    ),

    end_dt AS (
    SELECT 
        {{ src_hk }}
        , {{ src_ldts }}
        , COALESCE(LEAD(TIMESTAMP_SUB({{ src_ldts }}, INTERVAL 1 MICROSECOND)) OVER (PARTITION BY {{ src_hk }} ORDER BY {{ src_ldts }}),PARSE_TIMESTAMP('{{ timestamp_format }}', '{{ end_of_all_times }}')) as {{ ledts_alias }}
    FROM ord_ldts
    ),

    columns_to_select AS (
        SELECT 
            ms.{{ src_hk }}
        , ms.{{ src_ldts }}
        , endt.{{ ledts_alias }}
        , ms.{{ src_hd }}
        , ms.{{ src_ma }}
        {%- for column in all_columns -%}
            {%- if column.name not in exclude -%}
                {{ column.name }}
                {{ "," if not loop.last }}
            {%- endif -%}
        {%- endfor -%}
        FROM mas_data ms
        LEFT JOIN end_dt endt
        ON ms.{{ src_hk }} = endt.{{ src_hk }}
        AND ms.{{ src_ldts }} = endt.{{ src_ldts }}
    )

    SELECT * FROM columns_to_select

{%- endmacro -%}