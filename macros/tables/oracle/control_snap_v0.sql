{%- macro oracle__control_snap_v0(start_date, daily_snapshot_time, sdts_alias) -%}

{%- set timestamp_format = datavault4dbt.timestamp_format() -%}
{%- set date_format_std = 'YYYY-mm-dd' -%}
{%- set daily_snapshot_time = '0001-01-01 ' ~ daily_snapshot_time -%}
{%- set last_cte = '' -%}
WITH 
initial_timestamps AS 
(
    SELECT
            TO_DATE('{{ start_date }}', 'YYYY-mm-dd')
            + (EXTRACT(HOUR FROM {{ datavault4dbt.string_to_timestamp(timestamp_format, daily_snapshot_time) }}) / 24)
            + (EXTRACT(MINUTE FROM {{ datavault4dbt.string_to_timestamp(timestamp_format, daily_snapshot_time) }}) / 24 / 60)
            + level - 1 AS sdts
    FROM dual
    CONNECT BY level <= TRUNC(sysdate) - TO_DATE('2023-12-01', 'YYYY-mm-dd')
                        + (EXTRACT(HOUR FROM {{ datavault4dbt.string_to_timestamp(timestamp_format, daily_snapshot_time) }}) / 24)
                        + (EXTRACT(MINUTE FROM {{ datavault4dbt.string_to_timestamp(timestamp_format, daily_snapshot_time) }}) / 24 / 60)
                        + 2
    ORDER BY TO_DATE('{{ start_date }}', 'YYYY-mm-dd')
             + (EXTRACT(HOUR FROM {{ datavault4dbt.string_to_timestamp(timestamp_format, daily_snapshot_time) }}) / 24)
             + (EXTRACT(MINUTE FROM {{ datavault4dbt.string_to_timestamp(timestamp_format, daily_snapshot_time) }}) / 24 / 60)
             + level - 1

    {%- set last_cte = 'initial_timestamps' -%}
)


{%- if is_incremental() %}
, incremental_cte AS (
    SELECT 
        src.* 
    FROM initial_timestamps src

    WHERE src.sdts > (SELECT MAX(t.{{ sdts_alias }}) FROM {{ this }} t)
    {%- set last_cte = 'incremental_cte' -%}

)
{%- endif %}

, enriched_timestamps AS 
(
    SELECT
        sdts as {{ sdts_alias }},
        1 as force_active,
        sdts AS replacement_sdts,
        CONCAT('Snapshot ', TRUNC(TO_DATE(sdts),'DD')) AS caption,
        CASE
            WHEN EXTRACT(MINUTE FROM CAST(sdts AS TIMESTAMP)) = 0 AND EXTRACT(SECOND FROM CAST(sdts AS TIMESTAMP)) = 0
            THEN 1
            ELSE 0
        END AS is_hourly,
        CASE
            WHEN EXTRACT(MINUTE FROM CAST(sdts AS TIMESTAMP)) = 0 AND EXTRACT(SECOND FROM CAST(sdts AS TIMESTAMP)) = 0 AND EXTRACT(HOUR FROM CAST(sdts AS TIMESTAMP)) = 0
            THEN 1
            ELSE 0
        END AS is_daily,
        CASE 
            WHEN to_char(sdts, 'ID') = '1' THEN 1
            ELSE 0
        END AS is_weekly, 
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 THEN 1
            ELSE 0
        END AS is_monthly,
        CASE
            WHEN sdts = TRUNC(sdts, 'MM') + INTERVAL '1' MONTH + INTERVAL '-1' DAY THEN 1
            ELSE 0
        END AS is_end_of_month,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 AND EXTRACT(MONTH FROM sdts) IN (1,4,7,10) THEN 1
            ELSE 0
        END AS is_quarterly,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 AND EXTRACT(MONTH FROM sdts) = 1 THEN 1
            ELSE 0
        END AS is_yearly,
        CASE
            WHEN EXTRACT(DAY FROM sdts)=31 AND EXTRACT(MONTH FROM sdts) = 12 THEN 1
            ELSE 0
        END AS is_end_of_year,
        CAST(NULL AS VARCHAR2(2000)) AS "comment"
    FROM 
        {{ last_cte }}
)

SELECT * FROM enriched_timestamps

{%- endmacro -%}
