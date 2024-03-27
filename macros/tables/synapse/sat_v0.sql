{%- macro synapse__sat_v0(parent_hashkey, src_hashdiff, src_payload, src_ldts, src_rsrc, source_model, disable_hwm, source_is_single_batch) -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- set ns=namespace(src_hashdiff="", hdiff_alias="") %}

{%- if  src_hashdiff is mapping and src_hashdiff is not none -%}
    {% set ns.src_hashdiff = src_hashdiff["source_column"] %}
    {% set ns.hdiff_alias = src_hashdiff["alias"] %}
{% else %}
    {% set ns.src_hashdiff = src_hashdiff %}
    {% set ns.hdiff_alias = src_hashdiff  %}
{%- endif -%}

{%- set source_cols = datavault4dbt.expand_column_list(columns=[src_rsrc, src_ldts, src_payload]) -%}

{%- set source_relation = ref(source_model) -%}

{{ datavault4dbt.prepend_generated_by() }}

WITH

{# Selecting all source data, that is newer than latest data in sat if incremental #}
source_data AS (

    SELECT
        {{ parent_hashkey }},
        {{ ns.src_hashdiff }} as {{ ns.hdiff_alias }},
        {{ datavault4dbt.print_list(source_cols) }}
    FROM {{ source_relation }}

    {%- if is_incremental() and not disable_hwm %}
    WHERE {{ src_ldts }} > (
        SELECT
            COALESCE(MAX({{ src_ldts }}), {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }}) FROM {{ this }}
        WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
    )
    {%- endif %}

    {%- set source_cte = 'source_data' -%}

),

{# Get the latest record for each parent hashkey in existing sat, if incremental. #}
{%- if is_incremental() %}
distinct_incoming_hashkeys AS (

    SELECT DISTINCT
        {{ parent_hashkey }}
    FROM source_data

),

latest_entries_in_sat_prep AS (

    SELECT
        tgt.{{ parent_hashkey }},
        tgt.{{ ns.hdiff_alias }},
        ROW_NUMBER() OVER(PARTITION BY tgt.{{ parent_hashkey|lower }} ORDER BY tgt.{{ src_ldts }} DESC) as rn
    FROM {{ this }} tgt
    INNER JOIN distinct_incoming_hashkeys src
        ON tgt.{{ parent_hashkey }} = src.{{ parent_hashkey }}
),

latest_entries_in_sat AS (

    SELECT
        {{ parent_hashkey }},
        {{ ns.hdiff_alias }}
    FROM 
        latest_entries_in_sat_prep
    WHERE rn = 1  
),
{%- endif %}

{%- if not source_is_single_batch %}
{#
    Deduplicate source by comparing each hashdiff to the hashdiff of the previous record, for each hashkey.
    Additionally adding a row number based on that order, if incremental.
#}
deduplicated_numbered_source_prep AS (

    SELECT
    {{ parent_hashkey }},
    {{ ns.hdiff_alias }},
    {{ datavault4dbt.print_list(source_cols) }}
    , LAG({{ ns.hdiff_alias }}) OVER(PARTITION BY {{ parent_hashkey|lower }} ORDER BY {{ src_ldts }}) as prev_hashdiff
    FROM source_data

),

deduplicated_numbered_source AS (

    SELECT
    {{ parent_hashkey }},
    {{ ns.hdiff_alias }},
    {{ datavault4dbt.print_list(source_cols) }}
    {% if is_incremental() -%}
    , ROW_NUMBER() OVER(PARTITION BY {{ parent_hashkey }} ORDER BY {{ src_ldts }}) as rn
    {%- endif %}
    FROM deduplicated_numbered_source_prep
    WHERE 1=1
        AND {{ ns.hdiff_alias }} <> prev_hashdiff OR prev_hashdiff IS NULL

    {%- set source_cte = 'deduplicated_numbered_source' -%}

),

{% endif -%}

{#
    Select all records from the previous CTE. If incremental, compare the oldest incoming entry to
    the existing records in the satellite.
#}
records_to_insert AS (

    SELECT
    {{ parent_hashkey }},
    {{ ns.hdiff_alias }},
    {{ datavault4dbt.print_list(source_cols) }}
    FROM {{ source_cte }} sc
    {%- if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM latest_entries_in_sat
        WHERE {{ datavault4dbt.multikey(parent_hashkey, prefix=['latest_entries_in_sat', 'sc'], condition='=') }}
            AND {{ datavault4dbt.multikey(ns.hdiff_alias, prefix=['latest_entries_in_sat', 'sc'], condition='=') }}
            {%- if not source_is_single_batch %}
            AND sc.rn = 1
            {%- endif %}
    )
    {%- endif %}

    )

SELECT * FROM records_to_insert

{%- endmacro -%}