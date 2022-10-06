{%- macro exasol__sat_v1(sat_v0, hashkey, hashdiff, src_ldts, src_rsrc, ledts_alias, add_is_current_flag) -%}

{%- set end_of_all_times = var('datavault4dbt.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('datavault4dbt.timestamp_format', 'YYYY-MM-DDTHH-MI-SS') -%}

{%- set hash = var('datavault4dbt.hash', 'MD5') -%}

{%- set is_current_col_alias = var('datavault4dbt.is_current_col_alias', 'IS_CURRENT') -%}

{%- set hash_dtype = var('datavault4dbt.hash_datatype', 'STRING') -%}
{%- set hash_default_values = fromjson(datavault4dbt.hash_default_values(hash_function=hash,hash_datatype=hash_dtype)) -%}
{%- set hash_alg = hash_default_values['hash_alg'] -%}
{%- set unknown_key = hash_default_values['unknown_key'] -%}
{%- set error_key = hash_default_values['error_key'] -%}

{%- set source_relation = ref(sat_v0) -%}

{%- set all_columns = datavault4dbt.source_columns(source_relation=source_relation) -%}
{%- set exclude = [hashkey, hashdiff, src_ldts, src_rsrc] -%}

{%- set source_columns_to_select = datavault4dbt.process_columns_to_select(all_columns, exclude) -%}

{{ datavault4dbt.prepend_generated_by() }}

WITH

{# Calculate ledts based on the ldts of the earlier record. #}
end_dated_source AS (

    SELECT
        {{ hashkey }},
        {{ hashdiff }},
        {{ src_rsrc }},
        {{ src_ldts }},
        COALESCE(LEAD(ADD_SECONDS({{ src_ldts }}, -0.001)) OVER (PARTITION BY {{ hashkey }} ORDER BY {{ src_ldts }}),{{ datavault4dbt.string_to_timestamp( timestamp_format , end_of_all_times) }}) as {{ ledts_alias }},
        {{ datavault4dbt.print_list(source_columns_to_select) }}
    FROM {{ source_relation }}

)

SELECT
    {{ hashkey }},
    {{ hashdiff }},
    {{ src_rsrc }},
    {{ src_ldts }},
    {{ ledts_alias }},
    {%- if add_is_current_flag %}
        CASE WHEN {{ ledts_alias }} = {{ datavault4dbt.string_to_timestamp( timestamp_format , end_of_all_times) }}
        THEN TRUE
        ELSE FALSE
        END AS {{ is_current_col_alias }},
    {% endif -%}
    {{ datavault4dbt.print_list(source_columns_to_select) }}
FROM end_dated_source

{%- endmacro -%}
