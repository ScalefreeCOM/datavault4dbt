{%- macro exasol__sat_v0(parent_hashkey, src_hashdiff, src_payload, src_ldts, src_rsrc, source_model) -%}

{%- set hash = var('dbtvault_scalefree.hash', 'MD5') -%}
{%- set hash_alg, unknown_key, error_key = dbtvault_scalefree.hash_default_values(hash_function=hash) -%}

{%- set beginning_of_all_times = var('dbtvault_scalefree.beginning_of_all_times', '0001-01-01T00-00-01') -%}
{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', 'YYYY-mm-ddTHH-MI-SS') -%}
{%- set ns=namespace(src_hashdiff="", hdiff_alias="") %}
{%- if  src_hashdiff is mapping and src_hashdiff is not none -%}
    {% set ns.src_hashdiff = src_hashdiff["source_column"] %}
    {% set ns.hdiff_alias = src_hashdiff["alias"] %}
{% else %}
    {% set ns.src_hashdiff = src_hashdiff %}
    {% set ns.hdiff_alias = src_hashdiff  %}
{%- endif -%}
{%- set source_cols = dbtvault_scalefree.expand_column_list(columns=[src_rsrc, src_ldts, src_payload]) -%}

{%- set source_relation = ref(source_model) -%}

{{ dbtvault_scalefree.prepend_generated_by() }}

WITH

{# Selecting all source data, that is newer than latest data in sat if incremental #}
source_data AS (

    SELECT
        {{ parent_hashkey }},
        {{ ns.src_hashdiff }} as {{ ns.hdiff_alias }},
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
{# Get the latest record for each parent hashkey in existing sat, if incremental. #}
latest_entries_in_sat AS (

    SELECT
        {{ parent_hashkey }},
        {{ ns.hdiff_alias }}
    FROM {{ this }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY {{ parent_hashkey|lower }} ORDER BY {{ src_ldts }} DESC) = 1

    ),

{%- endif %}

{#
    Deduplicate source by comparing each hashdiff to the hashdiff of the previous record, for each hashkey.
    Additionally adding a row number based on that order, if incremental.
#}
deduplicated_numbered_source AS (

    SELECT
    {{ parent_hashkey }},
    {{ ns.hdiff_alias }},
    {{ dbtvault_scalefree.print_list(source_cols) }}
    {% if is_incremental() -%}
     , ROW_NUMBER() OVER(PARTITION BY {{ parent_hashkey|lower }} ORDER BY {{ src_ldts }}) as rn
    {%- endif %}
    FROM source_data
    QUALIFY
        CASE
            WHEN {{ ns.hdiff_alias }} = LAG({{ ns.hdiff_alias }}) OVER(PARTITION BY {{ parent_hashkey|lower }} ORDER BY {{ src_ldts }}) THEN FALSE
            ELSE TRUE
        END
),

{#
    Select all records from the previous CTE. If incremental, compare the oldest incoming entry to
    the existing records in the satellite.
#}
records_to_insert AS (

    SELECT
    {{ parent_hashkey }},
    {{ ns.hdiff_alias }},
    {{ dbtvault_scalefree.print_list(source_cols) }}
    FROM deduplicated_numbered_source
    {%- if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM latest_entries_in_sat
        WHERE {{ dbtvault_scalefree.multikey(parent_hashkey, prefix=['latest_entries_in_sat', 'deduplicated_numbered_source'], condition='=') }}
            AND {{ dbtvault_scalefree.multikey(ns.hdiff_alias, prefix=['latest_entries_in_sat', 'deduplicated_numbered_source'], condition='=') }}
            AND deduplicated_numbered_source.rn = 1)
    {%- endif %}

    )

SELECT * FROM records_to_insert

{%- endmacro -%}
