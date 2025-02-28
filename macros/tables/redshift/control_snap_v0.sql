{%- macro redshift__control_snap_v0(start_date, daily_snapshot_time, sdts_alias, end_date) -%}

{% if datavault4dbt.is_nothing(end_date) %}
    {% set end_date = 'current_date' %}
{% else %}
    {% set end_date = "'"~end_date~"'::timestamp" %}
{% endif %}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}
{%- set timestamp_value = start_date ~ ' ' ~ daily_snapshot_time -%}
{%- if not datavault4dbt.is_something(sdts_alias) -%}
    {%- set sdts_alias = var('datavault4dbt.sdts_alias', 'sdts') -%}
{%- endif -%}

with recursive generate_dates({{ sdts_alias }}) as (
	Select {{ datavault4dbt.string_to_timestamp(timestamp_format, timestamp_value) }} as {{ sdts_alias }}
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
        TRUE as force_active,
        sdts as replacement_sdts,
        CONCAT('Snapshot ', DATE(sdts)) as caption,
        CASE
            WHEN EXTRACT(m FROM sdts) = 0 AND EXTRACT(s FROM sdts) = 0 THEN TRUE
            ELSE FALSE
        END as is_hourly,
        CASE
            WHEN EXTRACT(m FROM sdts) = 0 AND EXTRACT(s FROM sdts) = 0 AND EXTRACT(h FROM sdts) = 0 THEN TRUE
            ELSE FALSE
        END as is_daily,
        CASE
            WHEN EXTRACT(dayofweek FROM  sdts) = 1 THEN TRUE
            ELSE FALSE
        END as is_weekly,
        CASE
            WHEN EXTRACT(d FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END as is_monthly,
        CASE
            WHEN EXTRACT(d FROM sdts) = 1 AND EXTRACT(mon FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END as is_yearly,
        NULL as comment
    FROM initial_timestamps

)

SELECT * FROM enriched_timestamps

{%- endmacro -%}
