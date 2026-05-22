{%- macro redshift__ref_sat_v0(parent_ref_keys, src_hashdiff, src_payload, src_ldts, src_rsrc, source_model, disable_hwm, source_is_single_batch, additional_columns) -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- set parent_ref_keys = datavault4dbt.expand_column_list(columns=[parent_ref_keys]) -%}

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

{{ datavault4dbt.prepend_generated_by() }}

WITH

{# Selecting all source data, that is newer than latest data in ref_sat if incremental #}
source_data AS (

    SELECT
        {% for ref_key in parent_ref_keys %}
        {{ref_key}},
        {% endfor %}
        {%- if has_hashdiff %}
        {{ ns.src_hashdiff }} as {{ ns.hdiff_alias }},
        {%- endif %}
        {{ datavault4dbt.print_list(source_cols) }}
    FROM {{ source_relation }}

    {%- if is_incremental() and not disable_hwm %}
    WHERE {{ src_ldts }} > (
        SELECT
            MAX({{ src_ldts }}) FROM {{ this }}
        WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
    )
    {%- endif %}
),

{# Get the latest record for each parent ref key combination in existing sat, if incremental. #}
{%- if is_incremental() %}
latest_entries_in_sat AS (

    SELECT
        {{ datavault4dbt.print_list(parent_ref_keys) }}
        {%- if dedup_column is not none -%},
        {{ dedup_column }}
        {%- endif %}
    FROM
        {{ this }} redshift_requires_an_alias_if_the_qualify_is_directly_after_the_from
    QUALIFY ROW_NUMBER() OVER(PARTITION BY {%- for ref_key in parent_ref_keys %} {{ref_key}} {%- if not loop.last %}, {% endif %}{% endfor %} ORDER BY {{ src_ldts }} DESC) = 1
),
{%- endif %}

{%- set last_cte = 'deduplicated_numbered_source' if payload_count > 0 else 'source_data' -%}
{%- if payload_count > 0 %}
{#
    Deduplicate source by comparing each hashdiff/payload value to the value of the previous record, for each parent ref key combination.
    Additionally adding a row number based on that order, if incremental.
#}
deduplicated_numbered_source AS (

    SELECT
    {% for ref_key in parent_ref_keys %}
    {{ref_key}},
    {% endfor %}
    {%- if has_hashdiff %}
    {{ dedup_column }},
    {%- endif %}
    {{ datavault4dbt.print_list(source_cols) }}
    {% if is_incremental() -%}
    , ROW_NUMBER() OVER(PARTITION BY {%- for ref_key in parent_ref_keys %} {{ref_key}} {%- if not loop.last %}, {% endif %}{% endfor %} ORDER BY {{ src_ldts }}) as rn
    {%- endif %}
    FROM source_data redshift_requires_an_alias_if_the_qualify_is_directly_after_the_from
    QUALIFY
        CASE
            WHEN {{ dedup_column }} = LAG({{ dedup_column }}) OVER(PARTITION BY {%- for ref_key in parent_ref_keys %} {{ref_key}} {%- if not loop.last %}, {% endif %}{% endfor %} ORDER BY {{ src_ldts }}) THEN FALSE
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
    {% for ref_key in parent_ref_keys %}
    {{ref_key}},
    {% endfor %}
    {%- if has_hashdiff %}
    {{ dedup_column }},
    {%- endif %}
    {{ datavault4dbt.print_list(source_cols) }}
    FROM {{ last_cte }}
    {%- if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM latest_entries_in_sat
        WHERE 1=1
            {% for ref_key in parent_ref_keys %}
            AND {{ datavault4dbt.multikey(ref_key, prefix=['latest_entries_in_sat', last_cte], condition='=') }}
            {% endfor %}
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
