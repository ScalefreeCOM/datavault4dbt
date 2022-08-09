{%- macro default__eff_sat_link_v1(eff_sat_link_v0, link_hashkey, driving_key, secondary_fks, src_ldts, src_rsrc) -%}

{%- set source_cols = dbtvault_scalefree.expand_column_list(columns=[link_hashkey, driving_key, secondary_fks, src_rsrc, src_ldts, 'is_active']) -%}

{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}

{%- set hash = var('dbtvault_scalefree.hash', 'MD5') -%}
{%- set hash_alg, unknown_key, error_key = dbtvault_scalefree.hash_default_values(hash_function=hash) -%}

{{ dbtvault_scalefree.prepend_generated_by() }}

WITH

source_data AS (

    SELECT
        {{ dbtvault.prefix(source_cols, 'sat_v0') }}
    FROM {{ ref(eff_sat_link_v0) }} AS sat_v0  

),

time_ranges AS (

    SELECT 
        {{ link_hashkey }},
        {{ driving_key }},
        

)

