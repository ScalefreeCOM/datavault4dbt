{%- macro sqlserver__control_snap_v1(control_snap_v0, log_logic, sdts_alias) -%}

WITH in_the_past as (

SELECT 
	*,
	ROW_NUMBER() OVER (ORDER BY {{ sdts_alias }} desc) as rn

FROM {{ ref(control_snap_v0) }}
WHERE {{ sdts_alias }} < GETDATE() 
),

dynamic as (SELECT 
	src.{{ sdts_alias }},
	src.{{ sdts_alias }}_date,
	src.force_active,
	CASE WHEN itp.{{ sdts_alias }} is not null THEN 1 ELSE 0 END AS is_in_the_past,
	CASE WHEN itp.rn = 1 THEN 1 ELSE 0 END AS is_current, 
	CASE WHEN src.year = DATEPART(YEAR, GETDATE()) THEN 1 ELSE 0 END as is_current_year, 
	CASE WHEN src.year = DATEPART(YEAR, GETDATE())-1 THEN 1 ELSE 0 END as is_last_year, 
	CASE WHEN DATEDIFF(day, src.{{ sdts_alias }}, GETDATE()) between 0 and 365 THEN 1 ELSE 0 END as is_current_rolling_year,
	CASE WHEN DATEDIFF(day, src.{{ sdts_alias }}, GETDATE()) between 366 and 730 THEN 1 ELSE 0 END as is_last_rolling_year,
	src.year,
	src.quarter,
	src.month,
	src.day_of_month,
	src.day_of_year,
	src.weekday,
	src.week,
	src.iso_week,
	src.is_end_of_week,
	src.is_end_of_month,
	src.is_end_of_quarter,
	src.is_end_of_year



FROM {{ ref(control_snap_v0) }} src
INNER JOIN in_the_past itp ON src.{{ sdts_alias }} = itp.{{ sdts_alias }})

SELECT * FROM dynamic

{%- endmacro -%}
