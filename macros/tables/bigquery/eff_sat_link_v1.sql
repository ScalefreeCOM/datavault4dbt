{%- macro default__eff_sat_link_v1(eff_sat_link_v0, link_hashkey, driving_key, secondary_fks, src_ldts, src_rsrc, eff_from_alias, eff_to_alias) -%}

{%- set source_cols = datavault4dbt.expand_column_list(columns=[link_hashkey, driving_key, secondary_fks, src_rsrc, src_ldts, 'is_active']) -%}
{%- set final_cols = datavault4dbt.expand_column_list(columns=[link_hashkey, driving_key, secondary_fks, src_rsrc, 'effective_from', 'effective_to']) -%}

{%- set driving_key = datavault4dbt.expand_column_list(columns=[driving_key]) -%}
{%- set secondary_fks = datavault4dbt.expand_column_list(columns=[secondary_fks]) -%}

{%- set end_of_all_times = var('datavault4dbt.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('datavault4dbt.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}

{%- set hash = var('datavault4dbt.hash', 'MD5') -%}
{%- set hash_alg, unknown_key, error_key = datavault4dbt.hash_default_values(hash_function=hash) -%}

{%- set source_relation = ref(eff_sat_link_v0) -%}

{{ datavault4dbt.prepend_generated_by() }}

WITH

source_data AS (

    SELECT
        {{ datavault4dbt.prefix(source_cols, 'sat_v0') }}
    FROM {{ source_relation }} AS sat_v0  

),

eff_ranges AS (

    SELECT 
        {{ link_hashkey }},
        {{ datavault4dbt.print_list(driving_key) }},
        {{ datavault4dbt.print_list(secondary_fks) }},
        {{ src_rsrc }},
        is_active,
        {{ src_ldts }} AS {{ eff_from_alias }},
        COALESCE(LAG(TIMESTAMP_SUB({{ src_ldts }}, INTERVAL 1 MICROSECOND)) OVER (PARTITION BY {{ link_hashkey }} ORDER BY ldts DESC), {{ datavault4dbt.string_to_timestamp( timestamp_format , end_of_all_times) }}) as {{ eff_to_alias }}
    FROM source_data

),

records_to_select AS (

    SELECT 
        {{ datavault4dbt.print_list(final_cols) }},
    FROM eff_ranges
    WHERE is_active = true

)

SELECT * FROM records_to_select
        
{%- endmacro -%}

