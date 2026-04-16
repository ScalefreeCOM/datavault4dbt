{%- macro trino__control_snap_v0(start_date, daily_snapshot_time, sdts_alias, end_date=none) -%}

{% if datavault4dbt.is_nothing(end_date) %}
  {% set end_date = 'CURRENT_TIMESTAMP' %}
{% else %}
    {% set end_date = "CAST('" ~ end_date ~ "' AS TIMESTAMP) + INTERVAL '1' DAY" %}
{% endif %}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- if not datavault4dbt.is_something(sdts_alias) -%}
    {%- set sdts_alias = var('datavault4dbt.sdts_alias', 'sdts') -%}
{%- endif -%}

WITH

initial_timestamps AS (
    
    SELECT
        sdts
    FROM 
        UNNEST(SEQUENCE(CAST('{{ start_date }} {{ daily_snapshot_time }}' AS TIMESTAMP), {{ end_date }}, INTERVAL '1' DAY)) AS t(sdts)
    {%- if is_incremental() %}
    WHERE
        sdts > (SELECT MAX({{ sdts_alias }}) FROM {{ this }})
    {%- endif %}

),

enriched_timestamps AS (

    SELECT
        sdts as {{ sdts_alias }},
        TRUE as force_active,
        sdts as replacement_sdts,
        CONCAT('Snapshot ', CAST(CAST(sdts AS DATE) AS VARCHAR)) as caption,
        CASE
            WHEN EXTRACT(MINUTE FROM sdts) = 0 AND EXTRACT(SECOND FROM sdts) = 0 THEN TRUE
            ELSE FALSE
        END as is_hourly,
        CASE
            WHEN EXTRACT(MINUTE FROM sdts) = 0 AND EXTRACT(SECOND FROM sdts) = 0 AND EXTRACT(HOUR FROM sdts) = 0 THEN TRUE
            ELSE FALSE
        END as is_daily,
        CASE
            WHEN EXTRACT(DAY_OF_WEEK FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END as is_weekly,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END as is_monthly,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 AND EXTRACT(MONTH FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END as is_yearly
    FROM initial_timestamps

)

SELECT * FROM enriched_timestamps

{%- endmacro -%}
