{%- macro snowflake__ref_sat_v0(parent_ref_keys, src_hashdiff, src_payload, src_ldts, src_rsrc, source_model, disable_hwm, source_is_single_batch) -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- set parent_ref_keys = datavault4dbt.expand_column_list(columns=[parent_ref_keys]) -%}

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

{# Get max(ldts) #}
{% if execute %}
    {%- if is_incremental() and not disable_hwm %}
        {% set max_ldts_query = 'SELECT COALESCE(MAX(' ~ src_ldts ~ '), ' ~ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) ~ ')  FROM ' ~ this ~' WHERE '~ src_ldts ~' < '~ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times)  %}
        {% set max_ldts_results = run_query(max_ldts_query) %}
        {% set max_ldts = max_ldts_results.columns[0].values()[0] %}
    {%- endif %}
{% endif %}

{{ datavault4dbt.prepend_generated_by() }}

WITH

{# Selecting all source data, that is newer than latest data in ref_sat if incremental #}
source_data AS (

    SELECT
        {{- "\n\n    " ~ datavault4dbt.print_list(datavault4dbt.escape_column_names(parent_ref_keys)) }},
        {{ ns.src_hashdiff }} as {{ ns.hdiff_alias }},
        {{- "\n\n    " ~ datavault4dbt.print_list(datavault4dbt.escape_column_names(source_cols)) if source_cols else " *" }}
    FROM {{ source_relation }}

    {%- if is_incremental() and not disable_hwm %}
    WHERE {{ src_ldts }} > '{{ max_ldts }}'
    {%- endif %}
),

{# Get the latest record for each parent ref key combination in existing sat, if incremental. #}
{%- if is_incremental() %}
latest_entries_in_sat AS (

    SELECT
        {{- "\n\n    " ~ datavault4dbt.print_list(datavault4dbt.escape_column_names(parent_ref_keys)) }},
        {{ ns.hdiff_alias }}
    FROM 
        {{ this }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY {{ datavault4dbt.print_list(datavault4dbt.escape_column_names(parent_ref_keys)) }} ORDER BY {{ src_ldts }} DESC) = 1  
),
{%- endif %}

{#
    Deduplicate source by comparing each hashdiff to the hashdiff of the previous record, for each parent ref key combination.
    Additionally adding a row number based on that order, if incremental.
#}
deduplicated_numbered_source AS (

    SELECT
        {{- "\n\n    " ~ datavault4dbt.print_list(datavault4dbt.escape_column_names(parent_ref_keys)) }},
        {{ ns.hdiff_alias }},
        {{- "\n\n    " ~ datavault4dbt.print_list(datavault4dbt.escape_column_names(source_cols)) if source_cols else " *" }}
    {% if is_incremental() -%}
    , ROW_NUMBER() OVER(PARTITION BY {{ datavault4dbt.print_list(datavault4dbt.escape_column_names(parent_ref_keys)) }} ORDER BY {{ src_ldts }}) as rn
    {%- endif %}
    FROM source_data
    QUALIFY
        CASE
            WHEN {{ ns.hdiff_alias }} = LAG({{ ns.hdiff_alias }}) OVER(PARTITION BY {{ datavault4dbt.print_list(datavault4dbt.escape_column_names(parent_ref_keys)) }} ORDER BY {{ src_ldts }}) THEN FALSE
            ELSE TRUE
        END
),

{#
    Select all records from the previous CTE. If incremental, compare the oldest incoming entry to
    the existing records in the satellite.
#}
records_to_insert AS (

    SELECT
        {{- "\n\n    " ~ datavault4dbt.print_list(datavault4dbt.escape_column_names(parent_ref_keys)) }},
        {{ ns.hdiff_alias }},
        {{- "\n\n    " ~ datavault4dbt.print_list(datavault4dbt.escape_column_names(source_cols)) if source_cols else " *" }}
    FROM deduplicated_numbered_source
    {%- if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM latest_entries_in_sat
        WHERE 1=1
            {% for ref_key in parent_ref_keys %}
            AND {{ datavault4dbt.multikey(ref_key, prefix=['latest_entries_in_sat', 'deduplicated_numbered_source'], condition='=') }}
            {% endfor %}
            AND {{ datavault4dbt.multikey(ns.hdiff_alias, prefix=['latest_entries_in_sat', 'deduplicated_numbered_source'], condition='=') }}
            AND deduplicated_numbered_source.rn = 1)
    {%- endif %}

    )

SELECT * FROM records_to_insert

{%- endmacro -%}
