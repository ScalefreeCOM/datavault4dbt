{%- macro synapse__control_snap_v0(start_date, end_date, daily_snapshot_time, sdts_alias) -%}
{{ log('start_date: '~ start_date, true)}}
WITH initial_timestamps AS (
    SELECT
        CAST(CAST('{{ start_date }}' AS VARCHAR) + ' ' + '{{ daily_snapshot_time }}' AS DATETIME) + CAST(rn - 1 AS INT) AS {{ sdts_alias }}
    FROM 
    (
        SELECT 
            TOP (DATEDIFF(DAY, '{{ start_date }}', '{{ end_date }}') + 1)
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
        FROM 
            sys.all_objects s1
        CROSS JOIN 
            sys.all_objects s2
    ) AS system_row
),

enriched_timestamps AS (

SELECT 
	CONVERT(datetime2, {{ sdts_alias }}) as {{ sdts_alias }},
    CONVERT(varchar, {{ sdts_alias }}, 23) as {{ sdts_alias }}_date,
	1 as force_active,
    {{ sdts_alias }} AS replacement_{{ sdts_alias }},
    CONCAT('Snapshot ', CONVERT(date, {{ sdts_alias }}, 23)) AS caption,
	DATEPART(YEAR, {{ sdts_alias }}) as year,
	DATEPART(QUARTER, {{ sdts_alias }}) as quarter, 
	DATEPART(MONTH, {{ sdts_alias }}) as month,
	DATEPART(DAY, {{ sdts_alias }}) as day_of_month,
	DATEPART(DAYOFYEAR, {{ sdts_alias }}) as day_of_year,
	DATEPART(WEEKDAY, {{ sdts_alias }}) as weekday,
	DATEPART(WEEK, {{ sdts_alias }}) as week,
	DATEPART(ISO_WEEK, {{ sdts_alias }}) as iso_week,
	CASE WHEN DATEPART(weekday, {{ sdts_alias }}) = 7 THEN 1 ELSE 0 END AS is_end_of_week,
	CASE WHEN LEAD(DATEPART(Day, {{ sdts_alias }}), 1) OVER (ORDER BY {{ sdts_alias }}) = 1 THEN 1 ELSE 0 END AS is_end_of_month,
	CASE WHEN LEAD(DATEPART(QUARTER, {{ sdts_alias }}), 1) OVER (ORDER BY {{ sdts_alias }}) != DATEPART(QUARTER, {{ sdts_alias }}) THEN 1 ELSE 0 END as is_end_of_quarter,
	CASE WHEN LEAD(DATEPART(Dayofyear, {{ sdts_alias }}), 1) OVER (ORDER BY {{ sdts_alias }}) = 1 THEN 1 ELSE 0 END AS is_end_of_year,
    NULL AS comment
FROM initial_timestamps )

SELECT * FROM enriched_timestamps

{% if is_incremental() -%}
WHERE {{ sdts_alias }} NOT IN (SELECT {{ sdts_alias }} FROM {{ this }})
{%- endif -%}

{%- endmacro -%}