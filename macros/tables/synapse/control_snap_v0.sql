{%- macro synapse__control_snap_v0(start_date, end_date, daily_snapshot_time, sdts_alias) -%}

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
    SELECT TOP (DATEDIFF(DAY, '{{ start_date }}', {{ end_date }}) + 1)
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
  CONVERT(varchar, {{ sdts_alias }}, 23) as {{ sdts_alias }}_date,
	1 as force_active,
  CONVERT(datetime2(6), {{ sdts_alias }}) AS replacement_{{ sdts_alias }},
  CONCAT('Snapshot ', CONVERT(date, {{ sdts_alias }}, 23)) AS caption,
  CASE WHEN DATEPART(HOUR, {{ sdts_alias }}) = 0 AND DATEPART(MINUTE, {{ sdts_alias }}) = 0 AND DATEPART(SECOND, {{ sdts_alias }}) = 0 THEN 1 ELSE 0 END AS is_hourly,
  CASE WHEN DATEPART(HOUR, {{ sdts_alias }}) = 0 AND DATEPART(MINUTE, {{ sdts_alias }}) = 0 AND DATEPART(SECOND, {{ sdts_alias }}) = 0 THEN 1 ELSE 0 END AS is_daily,
  CASE WHEN DATEPART(WEEKDAY, {{ sdts_alias }}) = 1 THEN 1 ELSE 0 END AS is_weekly, -- assuming 1 is Monday
  CASE WHEN DAY({{ sdts_alias }}) = 1 THEN 1 ELSE 0 END AS is_monthly,
  CASE WHEN LEAD(DATEPART(DAY, {{ sdts_alias }}), 1) OVER (ORDER BY {{ sdts_alias }}) = 1 THEN 1 ELSE 0 END AS is_end_of_month,
  CASE WHEN (MONTH({{ sdts_alias }}) IN (1, 4, 7, 10) AND DAY({{ sdts_alias }}) = 1) THEN 1 ELSE 0 END AS is_quarterly,
  CASE WHEN MONTH({{ sdts_alias }}) = 1 AND DAY({{ sdts_alias }}) = 1 THEN 1 ELSE 0 END AS is_yearly,
  CASE WHEN LEAD(DATEPART(DAYOFYEAR, {{ sdts_alias }}), 1) OVER (ORDER BY {{ sdts_alias }}) = 1 THEN 1 ELSE 0 END AS is_end_of_year,
  NULL AS comment
FROM initial_timestamps )

SELECT * FROM enriched_timestamps

{% if is_incremental() -%}
WHERE {{ sdts_alias }} NOT IN (SELECT {{ sdts_alias }} FROM ({{ this }}))
{%- endif -%}

{%- endmacro -%}