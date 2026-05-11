{%- macro redshift__control_snap_v0(start_date, daily_snapshot_time, sdts_alias, end_date=none) -%}

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

{%- set first_day_of_week_var = datavault4dbt.first_day_of_week() -%}

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
            WHEN EXTRACT(minute FROM sdts) = 0 AND EXTRACT(second FROM sdts) = 0 THEN TRUE
            ELSE FALSE
        END as is_hourly,
        CASE
            WHEN EXTRACT(minute FROM sdts) = 0 AND EXTRACT(second FROM sdts) = 0 AND EXTRACT(hour FROM sdts) = 0 THEN TRUE
            ELSE FALSE
        END as is_daily,
        CASE    
            WHEN (CASE WHEN EXTRACT(DOW FROM sdts) = 0 THEN 7 ELSE EXTRACT(DOW FROM sdts) END) = {{ first_day_of_week_var }} THEN TRUE
            ELSE FALSE
        END as is_beginning_of_week,
        CASE
            WHEN (CASE WHEN EXTRACT(DOW FROM sdts) = 0 THEN 7 ELSE EXTRACT(DOW FROM sdts) END) = {{ ((first_day_of_week_var + 5) % 7) + 1 }} THEN TRUE
            ELSE FALSE
        END as is_end_of_week,
        CASE
            WHEN EXTRACT(day FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END as is_beginning_of_month,
        CASE
            WHEN EXTRACT(day FROM (sdts + INTERVAL '1 day')) = 1 THEN TRUE
            ELSE FALSE
        END as is_end_of_month,
        CASE
            WHEN EXTRACT(month FROM sdts) IN (1, 4, 7, 10) AND EXTRACT(day FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END as is_beginning_of_quarter,
        CASE 
            WHEN EXTRACT(MONTH FROM sdts) IN (3, 6, 9, 12) AND EXTRACT(day FROM (sdts + INTERVAL '1 day')) = 1  THEN TRUE
            ELSE FALSE
        END AS is_end_of_quarter,
        CASE
            WHEN EXTRACT(day FROM sdts) = 1 AND EXTRACT(month FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END as is_beginning_of_year,
        CASE
            WHEN EXTRACT(month FROM sdts) = 12 AND EXTRACT(day FROM sdts) = 31 THEN TRUE
            ELSE FALSE
        END as is_end_of_year,
        NULL as comment
    FROM initial_timestamps

)

SELECT * FROM enriched_timestamps

{%- endmacro -%}