{%- macro oracle__control_snap_v0(start_date, daily_snapshot_time, sdts_alias, end_date=none) -%}

{%- set timestamp_format = datavault4dbt.timestamp_format() -%}
{%- set timestamp_value = start_date ~ ' ' ~ daily_snapshot_time -%}
{%- if not datavault4dbt.is_something(sdts_alias) -%}
    {%- set sdts_alias = var('datavault4dbt.sdts_alias', 'sdts') -%}
{%- endif -%}

with generate_dates({{ sdts_alias }}) as (
	Select {{ datavault4dbt.string_to_timestamp(timestamp_format, timestamp_value) }} as {{ sdts_alias }}
    from dual
  	union all
  	select {{ sdts_alias }} + 1
  	from generate_dates
  	where {{ sdts_alias }} < current_date
),

initial_timestamps AS (
    
    SELECT
        {{ sdts_alias }}
    FROM 
        generate_dates
    {%- if is_incremental() %}
    WHERE
        {{ sdts_alias }} > (SELECT MAX({{ sdts_alias }}) FROM {{ this }})
    {%- endif %}

),

enriched_timestamps AS (

    SELECT
        sdts as {{ sdts_alias }},
        1 as force_active,
        sdts AS replacement_sdts,
        CONCAT('Snapshot ', TO_CHAR(sdts, 'YYYY-MM-DD')) AS caption,
        CASE
            WHEN EXTRACT(MINUTE FROM sdts) = 0 AND EXTRACT(SECOND FROM sdts) = 0 THEN 1
            ELSE 0
        END as is_hourly,
        CASE
            WHEN EXTRACT(MINUTE FROM sdts) = 0 AND EXTRACT(SECOND FROM sdts) = 0 AND EXTRACT(HOUR FROM sdts) = 0 THEN 1
            ELSE 0
        END as is_daily,
        CASE
            WHEN TRUNC(sdts) - TRUNC(sdts, 'IW') + 1 = 1 THEN 1
            ELSE 0
        END AS is_weekly,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 THEN 1
            ELSE 0
        END AS is_monthly,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 AND EXTRACT(MONTH FROM sdts) = 1 THEN 1
            ELSE 0
        END AS is_yearly,
        CAST(NULL as VARCHAR2(40)) comment_text
    FROM initial_timestamps

)

SELECT * FROM enriched_timestamps

{%- endmacro -%}
