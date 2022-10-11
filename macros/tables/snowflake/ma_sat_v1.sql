{%- macro snowflake__ma_sat_v1(sat_v0, hashkey, hashdiff, ma_attribute, src_ldts, src_rsrc, ledts_alias, add_is_current_flag) -%}

{%- set end_of_all_times = var('datavault4dbt.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('datavault4dbt.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}
{%- set is_current_col_alias = var('datavault4dbt.is_current_col_alias', 'IS_CURRENT') -%}

{%- set source_relation = ref(sat_v0) -%}
{%- set all_columns = datavault4dbt.source_columns(source_relation=source_relation) -%}
{%- set exclude = datavault4dbt.expand_column_list(columns=[hashkey, hashdiff, ma_attribute, src_ldts]) -%}
{%- set ma_attributes = datavault4dbt.expand_column_list(columns=[ma_attribute]) -%}


{%- set source_columns_to_select = datavault4dbt.process_columns_to_select(all_columns, exclude) -%}

{{ datavault4dbt.prepend_generated_by() }}

WITH

{# Getting everything from the underlying v0 satellite. #}
source_satellite AS (

    SELECT *
    FROM {{ source_relation }}

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
        COALESCE(LEAD({{ src_ldts }} - INTERVAL '1 MICROSECOND') OVER (PARTITION BY {{ hashkey }} ORDER BY {{ src_ldts }}),{{ datavault4dbt.string_to_timestamp(timestamp_format['snowflake'],end_of_all_times['snowflake']) }}) as {{ ledts_alias }}
    FROM distinct_hk_ldts

),

{# End-date each source record, based on the end-date for each load. #}
end_dated_source AS (

    SELECT
        src.{{ hashkey }},
        src.{{ src_ldts }},
        edl.{{ ledts_alias }},
        src.{{ hashdiff }},
        {%- if add_is_current_flag %}
            CASE WHEN {{ ledts_alias }} = {{ datavault4dbt.string_to_timestamp(timestamp_format['default'], end_of_all_times['default']) }}
            THEN TRUE
            ELSE FALSE
            END AS {{ is_current_col_alias }},
        {% endif -%}        
        {{ datavault4dbt.print_list(ma_attributes) }},
        {{ datavault4dbt.print_list(source_columns_to_select) }}
    FROM source_satellite AS src
    LEFT JOIN end_dated_loads edl
        ON src.{{ hashkey }} = edl.{{ hashkey }}
        AND src.{{ src_ldts }} = edl.{{ src_ldts }}

)

SELECT * FROM end_dated_source

{%- endmacro -%}
