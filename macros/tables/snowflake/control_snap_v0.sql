{%- macro snowflake__control_snap_v0(start_date, daily_snapshot_time, sdts_alias, end_date=none) -%}

{%- set timestamp_format = datavault4dbt.timestamp_format() -%}
{%- set start_date = start_date | replace('00:00:00', daily_snapshot_time) -%}

WITH 

initial_timestamps AS (
    
    SELECT
        DATEADD(DAY, SEQ4(), 
        TIMESTAMPADD(SECOND, EXTRACT(SECOND FROM TO_TIME('{{ daily_snapshot_time }}')), 
            TIMESTAMPADD(MINUTE, EXTRACT(MINUTE FROM TO_TIME('{{ daily_snapshot_time }}')), 
                TIMESTAMPADD(HOUR, EXTRACT(HOUR FROM TO_TIME('{{ daily_snapshot_time }}')), TO_DATE('{{ start_date }}', 'YYYY-MM-DD')))
                ))::TIMESTAMP AS sdts
    FROM 
        TABLE(GENERATOR(ROWCOUNT => 100000))
    WHERE 
        sdts <= CURRENT_TIMESTAMP
    {%- if is_incremental() %}
    AND sdts > (SELECT MAX({{ sdts_alias }}) FROM {{ this }})
    {%- endif %}

),

enriched_timestamps AS (

    SELECT
        sdts as {{ sdts_alias }},
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
            WHEN EXTRACT(DAYOFWEEK FROM  sdts) = 1 THEN TRUE
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
