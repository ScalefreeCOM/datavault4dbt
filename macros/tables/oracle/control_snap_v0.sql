{%- macro oracle__control_snap_v0(start_date, daily_snapshot_time, sdts_alias, end_date=none) -%}

{% if datavault4dbt.is_nothing(end_date) %}
    {% set end_date = 'current_date' %}
{% else %}
    {% set end_date = "TO_DATE('"~end_date~"', '"~datavault4dbt.date_format()~"')" %}
{% endif %}

{%- set timestamp_format = datavault4dbt.timestamp_format() -%}
{%- set timestamp_value = start_date ~ ' ' ~ daily_snapshot_time -%}
{%- if not datavault4dbt.is_something(sdts_alias) -%}
    {%- set sdts_alias = var('datavault4dbt.sdts_alias', 'sdts') -%}
{%- endif -%}

{%- set first_day_of_week_var = var('datavault4dbt.first_day_of_week').get(target.type, 1) | int -%}

with generate_dates({{ sdts_alias }}) as (
	Select {{ datavault4dbt.string_to_timestamp(timestamp_format, timestamp_value) }} as {{ sdts_alias }}
    from dual
  	union all
  	select {{ sdts_alias }} + 1
  	from generate_dates
  	where {{ sdts_alias }} < {{ end_date }}
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
            {%- if first_day_of_week_var == 7 %}
            WHEN TRUNC(sdts) = TRUNC(sdts + 1, 'IW') - 1 THEN 1
            {%- else %}
            WHEN TRUNC(sdts) = TRUNC(sdts, 'IW') THEN 1
            {%- endif %}
            ELSE 0
        END AS is_beginning_of_week,
        CASE
            {%- if first_day_of_week_var == 7 %}
            WHEN TRUNC(sdts) = TRUNC(sdts + 1, 'IW') + 5 THEN 1
            {%- else %}
            WHEN TRUNC(sdts) = TRUNC(sdts, 'IW') + 6 THEN 1
            {%- endif %}
            ELSE 0
        END AS is_end_of_week,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 THEN 1
            ELSE 0
        END AS is_beginning_of_month,
        CASE 
            WHEN sdts = LAST_DAY(sdts) THEN 1 
            ELSE 0 
        END AS is_end_of_month,
        CASE 
            WHEN EXTRACT(MONTH FROM sdts) IN (1, 4, 7, 10) AND EXTRACT(DAY FROM sdts) = 1 THEN 1 
            ELSE 0 
        END AS is_beginning_of_quarter,
        CASE 
            WHEN EXTRACT(MONTH FROM sdts) IN (3, 6, 9, 12) AND sdts = LAST_DAY(sdts) THEN 1 
            ELSE 0 
        END AS is_end_of_quarter, 
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 AND EXTRACT(MONTH FROM sdts) = 1 THEN 1
            ELSE 0
        END AS is_beginning_of_year,
        CASE 
            WHEN EXTRACT(MONTH FROM sdts) = 12 AND EXTRACT(DAY FROM sdts) = 31 THEN 1 
            ELSE 0 
        END AS is_end_of_year,
        {# 
        Calculates week boundaries based on the configured start day. 
        ISO weeks (Monday) use 'IW' truncation. For Sunday starts, the date is 
        shifted by +1 day before the truncate (pushing Sunday into the 'next' Monday) 
        and then the resulting boundary is shifted back to Sunday (-1) or forward 
        to Saturday (+5).
        #}
        {%- if first_day_of_week_var == 7 %}
        TRUNC(sdts + 1, 'IW') - 1 AS beginning_of_week,
        TRUNC(sdts + 1, 'IW') + 5 AS end_of_week,
        {%- else %}
        TRUNC(sdts, 'IW') AS beginning_of_week,
        TRUNC(sdts, 'IW') + 6 AS end_of_week,
        {%- endif %}
        TRUNC(sdts, 'MM') AS beginning_of_month,
        LAST_DAY(sdts) AS end_of_month,
        TRUNC(sdts, 'Q') AS beginning_of_quarter,
        ADD_MONTHS(TRUNC(sdts, 'Q'), 3) - 1 AS end_of_quarter,
        TRUNC(sdts, 'YYYY') AS beginning_of_year,
        ADD_MONTHS(TRUNC(sdts, 'YYYY'), 12) - 1 AS end_of_year,
        CAST(NULL as VARCHAR2(40)) comment_text
    FROM initial_timestamps

)

SELECT * FROM enriched_timestamps

{%- endmacro -%}