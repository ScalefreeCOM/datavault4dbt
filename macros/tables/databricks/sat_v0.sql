{%- macro databricks__sat_v0(parent_hashkey, src_hashdiff, src_payload, src_ldts, src_rsrc, source_model, disable_hwm, source_is_single_batch, additional_columns) -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- set src_payload = src_payload | default([], true) -%}
{%- set src_payload = [src_payload] if src_payload is string else src_payload -%}
{%- set payload_count = src_payload | length -%}
{%- set has_hashdiff = src_hashdiff is not none and src_hashdiff != '' -%}

{%- set ns=namespace(src_hashdiff="", hdiff_alias="") %}

{%- if has_hashdiff -%}
    {%- if src_hashdiff is mapping -%}
        {%- set ns.src_hashdiff = src_hashdiff["source_column"] -%}
        {%- set ns.hdiff_alias = src_hashdiff["alias"] -%}
    {%- else -%}
        {%- set ns.src_hashdiff = src_hashdiff -%}
        {%- set ns.hdiff_alias = src_hashdiff -%}
    {%- endif -%}
{%- endif -%}

{%- set dedup_column = ns.hdiff_alias if has_hashdiff else (src_payload[0] if payload_count == 1 else none) -%}

{# Select the additional_columns and put them in an array. If additional_colums none, then empty array #}
{%- set additional_columns = additional_columns | default([],true) -%}
{%- set additional_columns = [additional_columns] if additional_columns is string else additional_columns -%}

{%- set source_cols = datavault4dbt.expand_column_list(columns=[src_rsrc, src_ldts, src_payload, additional_columns]) -%}

{%- set source_relation = ref(source_model) -%}

{%- set src_ldts = datavault4dbt.escape_column_names(src_ldts) -%}
{%- set src_rsrc = datavault4dbt.escape_column_names(src_rsrc) -%}
{%- set parent_hashkey = datavault4dbt.escape_column_names(parent_hashkey) -%}
{%- if has_hashdiff -%}
    {%- set src_hashdiff = datavault4dbt.escape_column_names(src_hashdiff) -%}
{%- endif -%}

{{ datavault4dbt.prepend_generated_by() }}

WITH

{# Selecting all source data, that is newer than latest data in sat if incremental #}
source_data AS (

    SELECT
        {{ parent_hashkey }},
        {%- if has_hashdiff %}
        {{ ns.src_hashdiff }} as {{ ns.hdiff_alias }},
        {%- endif %}
        {{ datavault4dbt.print_list(source_cols) }}
    FROM {{ source_relation }}

    {%- if is_incremental() %}
    WHERE {{ src_ldts }} > (
        SELECT
            MAX({{ src_ldts }}) FROM {{ this }}
        WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
    )
    {%- endif %}
),

{# Get the latest record for each parent hashkey in existing sat, if incremental. #}
{%- if is_incremental() %}
latest_entries_in_sat AS (

    SELECT
        {{ parent_hashkey }}
        {%- if dedup_column is not none -%},
        {{ dedup_column }}
        {%- endif %}
    FROM
        {{ this }}
    WHERE 1=1

    {{ datavault4dbt.filter_latest_entries_in_sat() }}

    QUALIFY ROW_NUMBER() OVER(PARTITION BY {{ parent_hashkey|lower }} ORDER BY {{ src_ldts }} DESC) = 1
),
{%- endif %}

{%- set last_cte = 'deduplicated_numbered_source' if payload_count > 0 else 'source_data' -%}

{#
    Deduplicate source by comparing each hashdiff/payload value to the value of the previous record, for each hashkey.
    Additionally adding a row number based on that order, if incremental.
    Skipped entirely when no payload is provided (Modus C).
#}
{%- if payload_count > 0 %}
deduplicated_numbered_source AS (

    SELECT
    {{ parent_hashkey }},
    {%- if has_hashdiff %}
    {{ dedup_column }},
    {%- endif %}
    {{ datavault4dbt.print_list(source_cols) }}
    {% if is_incremental() -%}
    , ROW_NUMBER() OVER(PARTITION BY {{ parent_hashkey }} ORDER BY {{ src_ldts }}) as rn
    {%- endif %}
    FROM source_data
    QUALIFY
        CASE
            WHEN {{ dedup_column }} = LAG({{ dedup_column }}) OVER(PARTITION BY {{ parent_hashkey|lower }} ORDER BY {{ src_ldts }}) THEN FALSE
            ELSE TRUE
        END
),
{%- endif %}

{#
    Select all records from the previous CTE. If incremental, compare the oldest incoming entry to
    the existing records in the satellite.
#}
records_to_insert AS (

    SELECT
    {{ parent_hashkey }},
    {%- if has_hashdiff %}
    {{ dedup_column }},
    {%- endif %}
    {{ datavault4dbt.print_list(source_cols) }}
    FROM {{ last_cte }}
    {%- if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM latest_entries_in_sat
        WHERE {{ datavault4dbt.multikey(parent_hashkey, prefix=['latest_entries_in_sat', last_cte], condition='=') }}
        {%- if dedup_column is not none %}
            AND {{ datavault4dbt.multikey(dedup_column, prefix=['latest_entries_in_sat', last_cte], condition='=') }}
        {%- endif %}
        {%- if payload_count > 0 %}
            AND {{ last_cte }}.rn = 1
        {%- endif %})
    {%- endif %}

    )

SELECT * FROM records_to_insert

{%- endmacro -%}
