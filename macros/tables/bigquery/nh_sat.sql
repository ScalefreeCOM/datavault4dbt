{%- macro default__nh_sat(parent_hashkey, src_payload, src_ldts, src_rsrc, source_model) -%}

{%- set hash = var('dbtvault_scalefree.hash', 'MD5') -%}
{%- set hash_alg, unknown_key, error_key = dbtvault_scalefree.hash_default_values(hash_function=hash) -%}

{%- set beginning_of_all_times = var('dbtvault_scalefree.beginning_of_all_times', '0001-01-01T00-00-01') -%}
{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}

{%- set source_cols = dbtvault_scalefree.expand_column_list(columns=[parent_hashkey, src_ldts, src_rsrc, src_payload]) -%}

{%- set source_relation = ref(source_model) -%}

{{ dbtvault_scalefree.prepend_generated_by() }}

WITH

{# Selecting all source data, that is newer than latest data in sat if incremental #}
source_data AS (

    SELECT
        {{ dbtvault_scalefree.print_list(source_cols) }}
    FROM {{ source_relation }}

    {%- if is_incremental() %}
    WHERE {{ src_ldts }} > (
        SELECT
            MAX({{ src_ldts }}) FROM {{ this }}
        WHERE {{ src_ldts }} != {{ dbtvault_scalefree.string_to_timestamp(timestamp_format, end_of_all_times) }}
    )
    {%- endif %}
),

{% if is_incremental() -%}
{# Get distinct list of hashkeys inside the existing satellite, if incremental. #}
distinct_hashkeys AS (

    SELECT DISTINCT
        {{ parent_hashkey }}
    FROM {{ this }}

    ),

{%- endif %}

{#
    Select all records from the source. If incremental, insert only records, where the
    hashkey is not already in the existing satellite.
#}
records_to_insert AS (

    SELECT
        {{ dbtvault_scalefree.print_list(source_cols) }}
    FROM source_data
    {%- if is_incremental() %}
    WHERE {{ parent_hashkey }} NOT IN (SELECT * FROM distinct_hashkeys)
    {%- endif %}

    )

SELECT * FROM records_to_insert

{%- endmacro -%}
