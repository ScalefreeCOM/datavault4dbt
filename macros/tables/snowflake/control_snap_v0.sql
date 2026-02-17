{%- macro snowflake__control_snap_v0(start_date, daily_snapshot_time, sdts_alias, end_date=none) -%}

{% if datavault4dbt.is_nothing(end_date) %}
    {% set end_date = 'CURRENT_TIMESTAMP' %}
{% else %}
    {% set end_date = "DATEADD(day, 1, TO_DATE('"~end_date~"'))" %}
{% endif %}

{%- set timestamp_format = datavault4dbt.timestamp_format() -%}
{%- set start_date = start_date | replace('00:00:00', daily_snapshot_time) -%}

{%- set first_day_of_week_var = var('datavault4dbt.first_day_of_week').get(target.type, 1) | int -%}

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
        sdts <= {{ end_date }}
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
            {%- if first_day_of_week_var == 7 %}
            WHEN DAYOFWEEK_ISO(sdts) = 7 THEN TRUE
            {%- else %}
            WHEN DAYOFWEEK_ISO(sdts) = 1 THEN TRUE
            {%- endif %}
            ELSE FALSE
        END AS is_beginning_of_week,
        CASE
            {%- if first_day_of_week_var == 7 %}
            WHEN DAYOFWEEK_ISO(sdts) = 6 THEN TRUE
            {%- else %}
            WHEN DAYOFWEEK_ISO(sdts) = 7 THEN TRUE
            {%- endif %}
            ELSE FALSE
        END AS is_end_of_week,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END AS is_beginning_of_month,
        CASE
            WHEN CAST(sdts AS DATE) = LAST_DAY(sdts) THEN TRUE
            ELSE FALSE
        END AS is_end_of_month,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 AND EXTRACT(MONTH FROM sdts) IN (1, 4, 7, 10) THEN TRUE
            ELSE FALSE
        END AS is_beginning_of_quarter,
        CASE
            WHEN EXTRACT(MONTH FROM sdts) IN (3, 6, 9, 12) AND CAST(sdts AS DATE) = LAST_DAY(sdts) THEN TRUE
            ELSE FALSE
        END AS is_end_of_quarter,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 AND EXTRACT(MONTH FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END AS is_beginning_of_year,
        CASE
            WHEN EXTRACT(MONTH FROM sdts) = 12 AND EXTRACT(DAY FROM sdts) = 31 THEN TRUE
            ELSE FALSE
        END AS is_end_of_year,
        {# 
        Calculates week boundaries based on the configured start day. 
        Snowflake 'ISO_WEEK' truncation always returns Monday. To support a Sunday 
        start, the date is shifted forward by 1 day before the truncate (to push 
        Sunday into the 'next' ISO week) and then the resulting boundary is 
        shifted back to Sunday (-1) or forward to Saturday (+5).
        #}
        {%- if first_day_of_week_var == 7 %}
        CAST(DATEADD(day, -1, DATE_TRUNC('ISO_WEEK', DATEADD(day, 1, sdts))) AS DATE) as beginning_of_week,
        CAST(DATEADD(day, 5, DATE_TRUNC('ISO_WEEK', DATEADD(day, 1, sdts))) AS DATE) as end_of_week,
        {%- else %}
        CAST(DATE_TRUNC('ISO_WEEK', sdts) AS DATE) as beginning_of_week,
        CAST(DATEADD(day, 6, DATE_TRUNC('ISO_WEEK', sdts)) AS DATE) as end_of_week,
        {%- endif %}
        CAST(DATE_TRUNC('MONTH', sdts) AS DATE) as beginning_of_month,
        CAST(LAST_DAY(sdts) AS DATE) as end_of_month,
        CAST(DATE_TRUNC('QUARTER', sdts) AS DATE) as beginning_of_quarter,
        CAST(LAST_DAY(sdts, 'QUARTER') AS DATE) as end_of_quarter,
        CAST(DATE_TRUNC('YEAR', sdts) AS DATE) as beginning_of_year,
        CAST(LAST_DAY(sdts, 'YEAR') AS DATE) as end_of_year,
        NULL AS comment
    FROM initial_timestamps

)

SELECT * FROM enriched_timestamps

{%- endmacro -%}