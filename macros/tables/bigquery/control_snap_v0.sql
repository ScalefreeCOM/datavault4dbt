{%- macro default__control_snap_v0(start_date, daily_snapshot_time, sdts_alias, end_date=none) -%}

{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- if not datavault4dbt.is_something(sdts_alias) -%}
    {%- set sdts_alias = var('datavault4dbt.sdts_alias', 'sdts') -%}
{%- endif -%}

WITH

initial_timestamps AS (

    SELECT sdts
    FROM
        UNNEST(GENERATE_TIMESTAMP_ARRAY(
            TIMESTAMP_ADD(
                TIMESTAMP_ADD(
                    TIMESTAMP(PARSE_DATE('%Y-%m-%d', '{{ start_date }}')),
                INTERVAL EXTRACT(HOUR FROM TIME '{{ daily_snapshot_time }}') HOUR),
            INTERVAL EXTRACT(MINUTE FROM TIME '{{ daily_snapshot_time }}') MINUTE),
            TIMESTAMP_ADD(
                TIMESTAMP_ADD(
                    TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY),
                INTERVAL EXTRACT(HOUR FROM TIME '{{ daily_snapshot_time }}') HOUR),
            INTERVAL EXTRACT(MINUTE FROM TIME '{{ daily_snapshot_time }}') MINUTE),
        INTERVAL 1 DAY)) AS sdts

    {%- if is_incremental() %}
    WHERE sdts > (SELECT MAX({{ sdts_alias }}) FROM {{ this }})
    {%- endif -%}

),

enriched_timestamps AS (

    SELECT
        sdts as {{ sdts_alias }},
        TRUE as force_active,
        sdts as replacement_sdts,
        CONCAT("Snapshot ", DATE(sdts)) as caption,
        CASE
            WHEN EXTRACT(MINUTE FROM sdts) = 0 AND EXTRACT(SECOND FROM sdts) = 0 THEN TRUE
            ELSE FALSE
        END as is_hourly,
        CASE
            WHEN EXTRACT(MINUTE FROM sdts) = 0 AND EXTRACT(SECOND FROM sdts) = 0 AND EXTRACT(HOUR FROM sdts) = 0 THEN TRUE
            ELSE FALSE
        END as is_daily,
        CASE
            WHEN EXTRACT(DAYOFWEEK FROM  sdts) = 2 THEN TRUE
            ELSE FALSE
        END as is_weekly,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END as is_monthly,
        CASE 
            WHEN LAST_DAY(DATE(sdts), MONTH) = DATE(sdts) THEN TRUE
            ELSE FALSE
        END as is_end_of_month,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 AND EXTRACT(MONTH from sdts) IN (1,4,7,10) THEN TRUE
            ELSE FALSE
        END AS is_quarterly,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 AND EXTRACT(MONTH FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END as is_yearly,
        CASE
            WHEN LAST_DAY(DATE(sdts), YEAR) = DATE(sdts) THEN TRUE
            ELSE FALSE
        END AS is_end_of_year,
        NULL as comment
    FROM initial_timestamps

)

SELECT * FROM enriched_timestamps

{%- endmacro -%}
