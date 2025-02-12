{%- macro snowflake__ma_sat_v0(parent_hashkey, src_hashdiff, src_ma_key, src_payload, src_ldts, src_rsrc, source_model, disable_hwm, source_is_single_batch) -%}

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

{%- set source_cols = datavault4dbt.expand_column_list(columns=[src_rsrc, src_ldts, src_ma_key, src_payload]) -%}

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
    
    {% set source_cte = 'source_data' %}

),

{# Get the latest record for each parent hashkey in existing sat, if incremental. #}
{%- if is_incremental() %}
latest_entries_in_sat AS (

    SELECT
        {{ parent_hashkey }},
        {{ ns.hdiff_alias }}
    FROM 
        {{ this }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY {{ parent_hashkey|lower }} ORDER BY {{ src_ldts }} DESC) = 1  
),
{%- endif %}

{%- if not source_is_single_batch -%}
{# Get a list of all distinct hashdiffs that exist for each parent_hashkey. #}
deduped_row_hashdiff AS (

  SELECT 
    {{ parent_hashkey }},
    {{ src_ldts }},
    {{ ns.hdiff_alias }}
  FROM source_data
  QUALIFY CASE
            WHEN {{ ns.hdiff_alias }} = LAG({{ ns.hdiff_alias }}) OVER (PARTITION BY {{ parent_hashkey }} ORDER BY {{ src_ldts }}) THEN FALSE
            ELSE TRUE
          END
),

{# Dedupe the source data regarding non-delta groups. #}
deduped_rows AS (

  SELECT 
    source_data.{{ parent_hashkey }},
    source_data.{{ ns.hdiff_alias }},
    {{ datavault4dbt.alias_all(columns=source_cols, prefix='source_data') }}
  FROM source_data
  INNER JOIN deduped_row_hashdiff
    ON {{ datavault4dbt.multikey(parent_hashkey, prefix=['source_data', 'deduped_row_hashdiff'], condition='=') }}
    AND {{ datavault4dbt.multikey(src_ldts, prefix=['source_data', 'deduped_row_hashdiff'], condition='=') }}
    AND {{ datavault4dbt.multikey(ns.hdiff_alias, prefix=['source_data', 'deduped_row_hashdiff'], condition='=') }}

{%- set source_cte = 'deduped_rows' -%}
),
{%- endif %}

records_to_insert AS (

    SELECT
        {{ source_cte }}.{{ parent_hashkey }},
        {{ source_cte }}.{{ ns.hdiff_alias }},
        {{ datavault4dbt.alias_all(columns=source_cols, prefix=source_cte) }}
    FROM {{ source_cte }}
    {%- if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM latest_entries_in_sat
        WHERE {{ datavault4dbt.multikey(parent_hashkey, prefix=['latest_entries_in_sat', source_cte], condition='=') }}
            AND {{ datavault4dbt.multikey(ns.hdiff_alias, prefix=['latest_entries_in_sat', source_cte], condition='=') }} 
            )
    {%- endif %}

    )

SELECT * FROM records_to_insert

{%- endmacro -%}
