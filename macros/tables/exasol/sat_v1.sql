{%- macro exasol__sat_v1(sat_v0, hashkey, hashdiff, src_ldts, src_rsrc, ledts_alias, add_is_current_flag) -%}

{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', 'YYYY-MM-DDTHH-MI-SS') -%}

{%- set hash = var('dbtvault_scalefree.hash', 'MD5') -%}
{%- set is_current_col_alias = var('dbtvault_scalefree.is_current_col_alias', 'IS_CURRENT') -%}
{%- set hash_alg, unknown_key, error_key = dbtvault_scalefree.hash_default_values(hash_function=hash) -%}

{%- set source_relation = ref(sat_v0) -%}

{%- set all_columns = dbtvault_scalefree.source_columns(source_relation=source_relation) -%}
{%- set exclude = [hashkey, hashdiff, src_ldts, src_rsrc] -%}

{%- set source_columns_to_select = dbtvault_scalefree.process_columns_to_select(all_columns, exclude) -%}

{{ dbtvault_scalefree.prepend_generated_by() }}

WITH

{# Calculate ledts based on the ldts of the earlier record. #}
end_dated_source AS (

    SELECT
        {{ hashkey }},
        {{ hashdiff }},
        {{ src_rsrc }},
        {{ src_ldts }},
        COALESCE(LEAD(ADD_SECONDS({{ src_ldts }}, -0.001)) OVER (PARTITION BY {{ hashkey }} ORDER BY {{ src_ldts }}),{{ dbtvault_scalefree.string_to_timestamp( timestamp_format , end_of_all_times) }}) as {{ ledts_alias }},
        {{ dbtvault_scalefree.print_list(source_columns_to_select) }}
    FROM {{ source_relation }}

)

SELECT
    {{ hashkey }},
    {{ hashdiff }},
    {{ src_rsrc }},
    {{ src_ldts }},
    {{ ledts_alias }},
    {%- if add_is_current_flag %}
        CASE WHEN {{ ledts_alias }} = {{ dbtvault_scalefree.string_to_timestamp( timestamp_format , end_of_all_times) }}
        THEN TRUE
        ELSE FALSE
        END AS {{ is_current_col_alias }},
    {% endif -%}
    {{ dbtvault_scalefree.print_list(source_columns_to_select) }}
FROM end_dated_source

{%- endmacro -%}
