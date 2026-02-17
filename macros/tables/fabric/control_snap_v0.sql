{%- macro fabric__control_snap_v0(start_date, daily_snapshot_time, sdts_alias, end_date=none) -%}

{%- set first_day_of_week_var = var('datavault4dbt.first_day_of_week').get(target.type, 1) | int -%}

{% if datavault4dbt.is_nothing(end_date) %}
  {% set end_date = datavault4dbt.current_timestamp() %}
{% else %}
    {% set end_date = "'"~end_date~"'" %}
{% endif %}

WITH 

{#- To generate a large amount of row for creation of the date-series #}
initial_timestamps_prep AS (
    SELECT 1 AS num UNION ALL
    SELECT 2 UNION ALL
    SELECT 3 UNION ALL
    SELECT 4 UNION ALL
    SELECT 5 UNION ALL
    SELECT 6 UNION ALL
    SELECT 7 UNION ALL
    SELECT 8 UNION ALL
    SELECT 9 UNION ALL
    SELECT 10 UNION ALL
    SELECT 11 UNION ALL
    SELECT 12
		),

initial_timestamps AS (
    SELECT TOP (DATEDIFF(DAY, '{{ start_date }}', {{ end_date}}) + 1)
    DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1, '{{ start_date }}' ) AS {{ sdts_alias }}
    FROM initial_timestamps_prep s1
    CROSS JOIN initial_timestamps_prep s2
		CROSS JOIN initial_timestamps_prep s3
		CROSS JOIN initial_timestamps_prep s4
		CROSS JOIN initial_timestamps_prep s5
),

enriched_timestamps AS (

    SELECT 
        CONVERT(datetime2(6), {{ sdts_alias }}) as {{ sdts_alias }},
        1 as force_active,
        CONVERT(datetime2(6), {{ sdts_alias }}) AS replacement_{{ sdts_alias }},
        CONCAT('Snapshot ', CONVERT(date, {{ sdts_alias }}, 23)) AS caption,
        CASE 
            WHEN DATEPART(HOUR, {{ sdts_alias }}) = 0 AND DATEPART(MINUTE, {{ sdts_alias }}) = 0 AND DATEPART(SECOND, {{ sdts_alias }}) = 0 THEN 1 
            ELSE 0 
        END AS is_hourly,
        CASE 
            WHEN DATEPART(HOUR, {{ sdts_alias }}) = 0 AND DATEPART(MINUTE, {{ sdts_alias }}) = 0 AND DATEPART(SECOND, {{ sdts_alias }}) = 0 THEN 1 
            ELSE 0 
        END AS is_daily,
        CASE 
            {%- if first_day_of_week_var == 7 %}
            WHEN CAST({{ sdts_alias }} AS DATE) = CAST(DATEADD(day, -1, DATETRUNC(iso_week, DATEADD(day, 1, {{ sdts_alias }}))) AS DATE) THEN 1
            {%- else %}
            WHEN CAST({{ sdts_alias }} AS DATE) = CAST(DATETRUNC(iso_week, {{ sdts_alias }}) AS DATE) THEN 1
            {%- endif %}
            ELSE 0 
        END AS is_beginning_of_week,
        CASE 
            {%- if first_day_of_week_var == 7 %}
            WHEN CAST({{ sdts_alias }} AS DATE) = CAST(DATEADD(day, 5, DATETRUNC(iso_week, DATEADD(day, 1, {{ sdts_alias }}))) AS DATE) THEN 1
            {%- else %}
            WHEN CAST({{ sdts_alias }} AS DATE) = CAST(DATEADD(day, 6, DATETRUNC(iso_week, {{ sdts_alias }})) AS DATE) THEN 1
            {%- endif %}
            ELSE 0 
        END AS is_end_of_week,
        CASE 
            WHEN DAY({{ sdts_alias }}) = 1 THEN 1 
            ELSE 0 
        END AS is_beginning_of_month,
        CASE 
            WHEN EOMONTH({{ sdts_alias }}) = CAST({{ sdts_alias }} AS DATE) THEN 1 
            ELSE 0 
        END AS is_end_of_month,
        CASE 
            WHEN (MONTH({{ sdts_alias }}) IN (1, 4, 7, 10) AND DAY({{ sdts_alias }}) = 1) THEN 1 
            ELSE 0 
        END AS is_beginning_of_quarter,
        CASE 
            WHEN EOMONTH({{ sdts_alias }}) = CAST({{ sdts_alias }} AS DATE) AND MONTH({{ sdts_alias }}) IN (3, 6, 9, 12) THEN 1 
            ELSE 0 
        END AS is_end_of_quarter,
        CASE 
            WHEN MONTH({{ sdts_alias }}) = 1 AND DAY({{ sdts_alias }}) = 1 THEN 1 
            ELSE 0 
        END AS is_beginning_of_year,
        CASE 
            WHEN MONTH({{ sdts_alias }}) = 12 AND DAY({{ sdts_alias }}) = 31 THEN 1 
            ELSE 0 
        END AS is_end_of_year,
        {# 
        Calculates week boundaries based on the configured start day. 
        ISO weeks (Monday) use 'iso_week' truncation. For Sunday starts, the date is 
        shifted by +1 day before the truncate (pushing Sunday into the 'next' Monday) 
        and then the resulting boundary is shifted back to Sunday (-1) or forward 
        to Saturday (+5).
        #}
        {%- if first_day_of_week_var == 7 %}
        CAST(DATEADD(day, -1, DATETRUNC(iso_week, DATEADD(day, 1, {{ sdts_alias }}))) AS DATE) AS beginning_of_week,
        CAST(DATEADD(day, 5, DATETRUNC(iso_week, DATEADD(day, 1, {{ sdts_alias }}))) AS DATE) AS end_of_week,
        {%- else %}
        CAST(DATETRUNC(iso_week, {{ sdts_alias }}) AS DATE) AS beginning_of_week,
        CAST(DATEADD(day, 6, DATETRUNC(iso_week, {{ sdts_alias }})) AS DATE) AS end_of_week,
        {%- endif %}
        CAST(DATETRUNC(month, {{ sdts_alias }}) AS DATE) AS beginning_of_month,
        CAST(EOMONTH({{ sdts_alias }}) AS DATE) AS end_of_month,
        CAST(DATETRUNC(quarter, {{ sdts_alias }}) AS DATE) AS beginning_of_quarter,
        CAST(DATEADD(day, -1, DATEADD(quarter, 1, DATETRUNC(quarter, {{ sdts_alias }}))) AS DATE) AS end_of_quarter,
        CAST(DATETRUNC(year, {{ sdts_alias }}) AS DATE) AS beginning_of_year,
        CAST(DATEADD(day, -1, DATEADD(year, 1, DATETRUNC(year, {{ sdts_alias }}))) AS DATE) AS end_of_year,
        NULL AS comment
    FROM initial_timestamps 
)

SELECT * FROM enriched_timestamps

{% if is_incremental() -%}
WHERE {{ sdts_alias }} NOT IN (SELECT {{ sdts_alias }} FROM {{ this }})
{%- endif -%}

{%- endmacro -%}