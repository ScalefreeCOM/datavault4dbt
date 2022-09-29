{%- macro snowflake__control_snap_v0(start_date, daily_snapshot_time) -%}

{%- set timestamp_format = var('datavault4dbt.timestamp_format','%Y-%m-%dT%H-%M-%S') -%}

WITH 

initial_timestamps AS (
    
    SELECT
        DATEADD(DAY, SEQ4(), {{ datavault4dbt.string_to_timestamp(timestamp_format['snowflake'], start_date | replace('00:00:00','') ~ daily_snapshot_time) }})::TIMESTAMP AS sdts
    FROM 
        TABLE(GENERATOR(ROWCOUNT => 100000))
    WHERE 
        sdts <= CURRENT_TIMESTAMP
    {%- if is_incremental() %}
    AND sdts > (SELECT MAX(sdts) FROM {{ this }})
    {%- endif %}

),

enriched_timestamps AS (
    
    SELECT
        sdts,
        TRUE as force_active,
        sdts AS replacement_sdts,
        CONCAT('Snapshot ', DATE(sdts)) AS caption,
        CASE
            WHEN EXTRACT(MINUTE FROM sdts) = 0 AND EXTRACT(SECOND FROM sdts) = 0 THEN TRUE
            ELSE FALSE
        END AS is_hourly,
        CASE
            WHEN EXTRACT(MINUTE FROM sdts) = 0 AND EXTRACT(SECOND FROM sdts) = 0 AND EXTRACT(HOUR FROM sdts) = 0 THEN TRUE
            ELSE FALSE
        END AS is_daily,
        CASE
            WHEN EXTRACT(DAYOFWEEK FROM  sdts) = 2 THEN TRUE
            ELSE FALSE
        END AS is_weekly,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END AS is_monthly,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 AND EXTRACT(MONTH FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END AS is_yearly,
        NULL AS comment
    FROM initial_timestamps
)

SELECT * FROM enriched_timestamps

{%- endmacro -%}
