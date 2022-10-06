{%- macro default__sat_v0(parent_hashkey, src_hashdiff, src_payload, src_ldts, src_rsrc, source_model) -%}

{%- set hash = var('datavault4dbt.hash', 'MD5') -%}
{%- set hash_dtype = var('datavault4dbt.hash_datatype', 'STRING') -%}
{%- set hash_default_values = fromjson(datavault4dbt.hash_default_values(hash_function=hash,hash_datatype=hash_dtype)) -%}
{%- set hash_alg = hash_default_values['hash_alg'] -%}
{%- set unknown_key = hash_default_values['unknown_key'] -%}
{%- set error_key = hash_default_values['error_key'] -%}

{%- set beginning_of_all_times = var('datavault4dbt.beginning_of_all_times', '0001-01-01T00-00-01') -%}
{%- set end_of_all_times = var('datavault4dbt.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('datavault4dbt.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}

{%- set source_cols = datavault4dbt.expand_column_list(columns=[parent_hashkey, src_hashdiff, src_ldts, src_rsrc, src_payload]) -%}

{%- set source_relation = ref(source_model) -%}

{{ datavault4dbt.prepend_generated_by() }}

WITH

{# Selecting all source data, that is newer than latest data in sat if incremental #}
source_data AS (

    SELECT
        {{ datavault4dbt.print_list(source_cols) }}
    FROM {{ source_relation }}

    {%- if is_incremental() %}
    WHERE {{ src_ldts }} > (
        SELECT
            MAX({{ src_ldts }}) FROM {{ this }}
        WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format['default'], end_of_all_times['default']) }}
    )
    {%- endif %}
),

{# Get the latest record for each parent hashkey in existing sat, if incremental. #}
{%- if is_incremental() %}
latest_entries_in_sat AS (

    SELECT
        {{ parent_hashkey }},
        {{ src_hashdiff }}
    FROM 
        {{ this }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY {{ parent_hashkey|lower }} ORDER BY {{ src_ldts }} DESC) = 1  
),
{%- endif %}

{#
    Deduplicate source by comparing each hashdiff to the hashdiff of the previous record, for each hashkey.
    Additionally adding a row number based on that order, if incremental.
#}
deduplicated_numbered_source AS (

    SELECT
        {{ datavault4dbt.print_list(source_cols) }}
    {% if is_incremental() %}
        ,ROW_NUMBER() OVER(PARTITION BY {{ parent_hashkey|lower }} ORDER BY {{ src_ldts }}) as rn,
    {%- endif %}
    FROM source_data
    QUALIFY
        CASE
            WHEN {{ src_hashdiff }} = LAG({{ src_hashdiff }}) OVER(PARTITION BY {{ parent_hashkey|lower }} ORDER BY {{ src_ldts }}) THEN FALSE
            ELSE TRUE
        END
),

{#
    Select all records from the previous CTE. If incremental, compare the oldest incoming entry to
    the existing records in the satellite.
#}
records_to_insert AS (

    SELECT
        {{ datavault4dbt.print_list(source_cols) }}
    FROM deduplicated_numbered_source
    {%- if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM latest_entries_in_sat
        WHERE {{ datavault4dbt.multikey(parent_hashkey, prefix=['latest_entries_in_sat', 'deduplicated_numbered_source'], condition='=') }}
            AND {{ datavault4dbt.multikey(src_hashdiff, prefix=['latest_entries_in_sat', 'deduplicated_numbered_source'], condition='=') }}
            AND deduplicated_numbered_source.rn = 1)
    {%- endif %}

    )


SELECT * FROM records_to_insert

{%- endmacro -%}
