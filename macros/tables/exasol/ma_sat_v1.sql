{%- macro exasol__ma_sat_v1(sat_v0, hashkey, hashdiff, ma_attribute, src_ldts, src_rsrc, ledts_alias) -%}

{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', 'YYYY-mm-ddTHH-MI-SS') -%}

{%- set hash = var('dbtvault_scalefree.hash', 'MD5') -%}
{%- set hash_alg, unknown_key, error_key = dbtvault_scalefree.hash_default_values(hash_function=hash) -%}

{%- set source_relation = ref(sat_v0) -%}
{%- set all_columns = dbtvault_scalefree.source_columns(source_relation=source_relation) -%}
{%- set exclude = dbtvault_scalefree.expand_column_list(columns=[hashkey, hashdiff, ma_attribute, src_ldts, src_rsrc]) -%}
{%- set ma_attributes = dbtvault_scalefree.expand_column_list(columns=[ma_attribute]) -%}


{%- set source_columns_to_select = dbtvault.process_columns_to_select(all_columns, exclude) -%}

{{ dbtvault_scalefree.prepend_generated_by() }}

WITH

{# Getting everything from the underlying v0 satellite. #}
source_satellite AS (

    SELECT src.*
    FROM {{ source_relation }} as src

),

{# Selecting all distinct loads per hashkey. #}
distinct_hk_ldts AS (

    SELECT DISTINCT
        {{ hashkey }},
        {{ src_ldts }}
    FROM source_satellite

),

{# End-dating each ldts for each hashkey, based on earlier ldts per hashkey. #}
end_dated_loads AS (

    SELECT
        {{ hashkey }},
        {{ src_ldts }},
        COALESCE(LEAD(ADD_SECONDS({{ src_ldts }}, -0.001)) OVER (PARTITION BY {{ hashkey }} ORDER BY {{ src_ldts }}),{{ dbtvault_scalefree.string_to_timestamp( timestamp_format , end_of_all_times) }}) as {{ ledts_alias }}
    FROM distinct_hk_ldts

),

{# End-date each source record, based on the end-date for each load. #}
end_dated_source AS (

    SELECT
        src.{{ hashkey }},
        src.{{ hashdiff }},
        src.{{ src_rsrc }},
        src.{{ src_ldts }},
        edl.{{ ledts_alias }},
        {{ dbtvault_scalefree.print_list(ma_attributes) }},
        {{ dbtvault_scalefree.print_list(source_columns_to_select) }}
    FROM source_satellite AS src
    LEFT JOIN end_dated_loads edl
        ON src.{{ hashkey }} = edl.{{ hashkey }}
        AND src.{{ src_ldts }} = edl.{{ src_ldts }}

)

SELECT * FROM end_dated_source

{%- endmacro -%}
