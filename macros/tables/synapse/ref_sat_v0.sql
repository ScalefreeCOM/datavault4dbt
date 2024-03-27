{%- macro synapse__ref_sat_v0(parent_ref_keys, src_hashdiff, src_payload, src_ldts, src_rsrc, source_model, disable_hwm, source_is_single_batch) -%}

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

{{ datavault4dbt.prepend_generated_by() }}

WITH

{# Selecting all source data, that is newer than latest data in ref_sat if incremental #}
source_data AS (

    SELECT
        {% for ref_key in parent_ref_keys %}
        {{ref_key}},
        {%- endfor %}
        {{ ns.src_hashdiff }} as {{ ns.hdiff_alias }},
        {{ datavault4dbt.print_list(source_cols) }}
    FROM {{ source_relation }}

    {%- if is_incremental()  and not disable_hwm %}
    WHERE {{ src_ldts }} > (
        SELECT
            COALESCE (MAX({{ src_ldts }}), {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }}) FROM {{ this }}
        WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
    )
    AND {{ src_ldts }} <> {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
    {%- endif %}
    
    {%- set source_cte = 'source_data' -%}

    -- UNION ALL ghost record 000000
),

{# Adding window functions row_number and lag to target #}
{%- if is_incremental() %}
target_data AS (

    SELECT
        {% for ref_key in parent_ref_keys %}
        {{ref_key}},
        {% endfor %}
        {{ ns.hdiff_alias }}
        , ROW_NUMBER() OVER(PARTITION BY {%- for ref_key in parent_ref_keys %} {{ref_key}} {%- if not loop.last %}, {% endif %}{% endfor %} ORDER BY {{ src_ldts }} DESC) AS rn
    FROM 
        {{ this }}
),
{%- endif %}

{# Get the latest record for each parent ref key combination in existing sat, if incremental. #}
{%- if is_incremental() %}
latest_entries_in_sat AS (

    SELECT
        {% for ref_key in parent_ref_keys %}
        {{ref_key}},
        {% endfor %}
        {{ ns.hdiff_alias }}
    FROM 
        target_data
    WHERE rn = 1
),
{%- endif %}

{%- if not source_is_single_batch %}

{#
    Adding hashdiff of the previous record, for each parent ref key combination.
#}
numbered_source AS (

    SELECT
    {% for ref_key in parent_ref_keys %}
    {{ref_key}},
    {% endfor %}
    {{ ns.hdiff_alias }},
    {{ datavault4dbt.print_list(source_cols) }}
    , LAG({{ ns.hdiff_alias }}) OVER(PARTITION BY {%- for ref_key in parent_ref_keys %} {{ref_key}} {%- if not loop.last %}, {% endif %}{% endfor %} ORDER BY {{ src_ldts }}) prev_hashdiff
    FROM source_data
),

{#
    Deduplicate source by comparing each hashdiff to the hashdiff of the previous record, for each parent ref key combination.
    Additionally adding a row number based on that order, if incremental.
#}
deduplicated_numbered_source AS (

    SELECT
    {% for ref_key in parent_ref_keys %}
    {{ref_key}},
    {% endfor %}
    {{ ns.hdiff_alias }},
    {{ datavault4dbt.print_list(source_cols) }}
    {% if is_incremental() -%}
    , ROW_NUMBER() OVER(PARTITION BY {%- for ref_key in parent_ref_keys %} {{ref_key}} {%- if not loop.last %}, {% endif %}{% endfor %} ORDER BY {{ src_ldts }}) as rn
    {%- endif %}
    FROM numbered_source
    WHERE {{ ns.hdiff_alias }} <> prev_hashdiff OR prev_hashdiff IS NULL

    {%- set source_cte = 'deduplicated_numbered_source' -%}

),

{% endif -%}

{#
    Select all records from the previous CTE. If incremental, compare the oldest incoming entry to
    the existing records in the satellite.
#}
records_to_insert AS (

    SELECT
    {% for ref_key in parent_ref_keys %}
    {{ref_key}},
    {% endfor %}
    {{ ns.hdiff_alias }},
    {{ datavault4dbt.print_list(source_cols) }}
    FROM {{ source_cte }} sc
    {%- if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM latest_entries_in_sat
        WHERE 1=1
            {% for ref_key in parent_ref_keys %}
            AND {{ datavault4dbt.multikey(ref_key, prefix=['latest_entries_in_sat', 'sc'], condition='=') }}
            {% endfor %}
            AND {{ datavault4dbt.multikey(ns.hdiff_alias, prefix=['latest_entries_in_sat', 'sc'], condition='=') }}
            {%- if not source_is_single_batch %}
            AND sc.rn = 1
            {%- endif %}
            )
    {%- endif %}

    )

SELECT * FROM records_to_insert

{%- endmacro -%}
