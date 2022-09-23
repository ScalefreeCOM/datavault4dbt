{%- macro default__sat_v1(sat_v0, hashkey, hashdiff, src_ldts, src_rsrc, ledts_alias) -%}

{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}

{%- set source_relation = ref(sat_v0) -%}

{%- set all_columns = dbtvault_scalefree.source_columns(source_relation=source_relation) -%}
{%- set exclude = [hashkey, hashdiff, src_ldts, src_rsrc] -%}

{%- set source_columns_to_select = dbtvault.process_columns_to_select(all_columns, exclude) -%}

{{ dbtvault_scalefree.prepend_generated_by() }}

WITH

{# Calculate ledts based on the ldts of the earlier record. #}
end_dated_source AS (
    
    SELECT
        {{ hashkey }},
        {{ src_rsrc }},
        {{ src_ldts }},
        COALESCE(LEAD(TIMESTAMP_SUB({{ src_ldts }}, INTERVAL 1 MICROSECOND)) OVER (PARTITION BY {{ hashkey }} ORDER BY {{ src_ldts }}),{{ dbtvault_scalefree.string_to_timestamp(timestamp_format['default'], end_of_all_times['default']) }}) as {{ ledts_alias }},
        {{ hashdiff }},
        {{ dbtvault_scalefree.print_list(source_columns_to_select) }}
    FROM {{ source_relation }}
    WHERE {{ hashdiff }} != '{{ error_key }}'

)

SELECT * FROM end_dated_source

{%- endmacro -%}
