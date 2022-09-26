{%- macro default__control_snap_v0(start_date, daily_snapshot_time) -%}

{%- set timestamp_format = var('datavault4dbt.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}

WITH

initial_timestamps AS (

    SELECT sdts
    FROM
        UNNEST(GENERATE_TIMESTAMP_ARRAY(
            {{ datavault4dbt.string_to_timestamp(timestamp_format['default'], start_date) }},
            TIMESTAMP_ADD(
                TIMESTAMP_ADD(
                    TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY),
                INTERVAL EXTRACT(HOUR FROM TIME '{{ daily_snapshot_time }}') HOUR),
            INTERVAL EXTRACT(MINUTE FROM TIME '{{ daily_snapshot_time }}') MINUTE),
        INTERVAL 1 DAY)) AS sdts

    {%- if is_incremental() %}
    WHERE sdts > (SELECT MAX(sdts) FROM {{ this }})
    {%- endif -%}

),

enriched_timestamps AS (

    SELECT
        sdts,
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
            WHEN EXTRACT(DAY FROM sdts) = 1 AND EXTRACT(MONTH FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END as is_yearly,
        CAST(NULL AS TIMESTAMP) as ldts,
        NULL as comment
    FROM initial_timestamps

)

SELECT * FROM enriched_timestamps

{%- endmacro -%}
