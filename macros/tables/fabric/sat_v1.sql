{%- macro fabric__sat_v1(sat_v0, hashkey, hashdiff, src_ldts, src_rsrc, ledts_alias, add_is_current_flag, include_payload) -%}

{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- set is_current_col_alias = var('datavault4dbt.is_current_col_alias', 'is_current') -%}

{%- set source_relation = ref(sat_v0) -%}

{%- set all_columns = datavault4dbt.source_columns(source_relation=source_relation) -%}
{%- set exclude = [hashkey, hashdiff, src_ldts, src_rsrc] -%}

{%- set source_columns_to_select = datavault4dbt.process_columns_to_select(all_columns, exclude) -%}

{%- set source_columns_to_select = datavault4dbt.escape_column_names(source_columns_to_select) -%}

{% set src_ldts = datavault4dbt.escape_column_names(src_ldts) %}
{% set src_rsrc = datavault4dbt.escape_column_names(src_rsrc) %}
{% set ledts_alias = datavault4dbt.escape_column_names(ledts_alias) %}
{% set hashkey = datavault4dbt.escape_column_names(hashkey) %}
{% set hashdiff = datavault4dbt.escape_column_names(hashdiff) %}

WITH

{{ datavault4dbt.prepend_generated_by() }}

{# Calculate ledts based on the ldts of the earlier record. #}
end_dated_source AS (

    SELECT
        {{ hashkey }},
        {{ hashdiff }},
        {{ src_rsrc }},
        {{ src_ldts }},
        COALESCE(LEAD(DATEADD(ns, -100, {{ src_ldts }})) OVER (PARTITION BY {{ hashkey }} ORDER BY {{ src_ldts }}),{{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}) AS {{ ledts_alias }}
       {%- if source_columns_to_select | length >= 1 -%} , {% endif -%}
        {{ datavault4dbt.print_list(source_columns_to_select) }}
    FROM {{ source_relation }}

)

SELECT
    {{ hashkey }},
    {{ hashdiff }},
    {{ src_rsrc }},
    {{ src_ldts }},
    {{ ledts_alias }}
    {%- if source_columns_to_select | length >= 1 or add_is_current_flag -%} , {% endif -%}
    {%- if add_is_current_flag %}
        CASE WHEN {{ ledts_alias }} = {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
          THEN 1
          ELSE 0
        END AS {{ is_current_col_alias }}
        {%- if source_columns_to_select | length >= 1 -%} , {% endif -%}
    {% endif -%}
    {{ datavault4dbt.print_list(source_columns_to_select) }}
FROM end_dated_source

{%- endmacro -%}
