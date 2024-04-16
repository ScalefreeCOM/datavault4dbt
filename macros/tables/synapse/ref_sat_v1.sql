{%- macro synapse__ref_sat_v1(ref_sat_v0, ref_keys, hashdiff, src_ldts, src_rsrc, ledts_alias, add_is_current_flag) -%}

{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- set is_current_col_alias = var('datavault4dbt.is_current_col_alias', 'IS_CURRENT') -%}
{%- set ledts_alias = var('datavault4dbt.ledts_alias', 'ledts') -%}

{%- set source_relation = ref(ref_sat_v0) -%}

{%- set ref_keys = datavault4dbt.expand_column_list(columns=[ref_keys]) -%}

{%- set all_columns = datavault4dbt.source_columns(source_relation=source_relation) -%}
{%- set exclude = ref_keys + [hashdiff, src_ldts, src_rsrc] -%}

{%- set source_columns_to_select = datavault4dbt.process_columns_to_select(all_columns, exclude) -%}

{{ datavault4dbt.prepend_generated_by() }}

WITH

{# Calculate ledts based on the ldts of the earlier record. #}
end_dated_source AS (

    SELECT
        {% for ref_key in ref_keys %}
        {{ref_key}},
        {% endfor %}
        {{ hashdiff }},
        {{ src_rsrc }},
        {{ src_ldts }},
        COALESCE(LEAD(DATEADD(ns, -100, {{ src_ldts }})) OVER (PARTITION BY {%- for ref_key in ref_keys %} {{ref_key}} {%- if not loop.last %}, {% endif %}{% endfor %} ORDER BY {{ src_ldts }}),{{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}) AS {{ ledts_alias }},
        {{ datavault4dbt.print_list(source_columns_to_select) }}
    FROM {{ source_relation }}

)

SELECT
    {% for ref_key in ref_keys %}
    {{ref_key}},
    {% endfor %}
    {{ hashdiff }},
    {{ src_rsrc }},
    {{ src_ldts }},
    {{ ledts_alias }},
    {%- if add_is_current_flag %}
        CASE WHEN {{ ledts_alias }} = {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
        THEN 1
        ELSE 0
        END AS {{ is_current_col_alias }},
    {% endif -%}
    {{ datavault4dbt.print_list(source_columns_to_select) }}
FROM end_dated_source

{%- endmacro -%}
