{%- macro default__control_snap_v0(start_date, daily_snapshot_time, sdts_alias, end_date=none) -%}

{% if datavault4dbt.is_nothing(end_date) %}
    {% set end_date = 'CURRENT_TIMESTAMP()' %}
{% else %}
    {% set end_date = "'"~end_date~"'" %}
{% endif %}

{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- if not datavault4dbt.is_something(sdts_alias) -%}
    {%- set sdts_alias = var('datavault4dbt.sdts_alias', 'sdts') -%}
{%- endif -%}

{%- set first_day_of_week_var = var('datavault4dbt.first_day_of_week').get(target.type, 1) | int -%}

{%- set bigquery_day_of_week_target = (first_day_of_week_var % 7) + 1 -%}

{%- if first_day_of_week_var == 7 -%}
    {%- set bigquery_day_of_week_arg = 'WEEK(SUNDAY)' -%}
{%- else -%}
    {%- set bigquery_day_of_week_arg = 'WEEK(MONDAY)' -%}
{%- endif -%}

WITH

initial_timestamps AS (

    SELECT sdts
    FROM
        UNNEST(GENERATE_TIMESTAMP_ARRAY(
            TIMESTAMP_ADD(
                TIMESTAMP_ADD(
                    TIMESTAMP(PARSE_DATE('%Y-%m-%d', '{{ start_date }}')),
                INTERVAL EXTRACT(HOUR FROM TIME '{{ daily_snapshot_time }}') HOUR),
            INTERVAL EXTRACT(MINUTE FROM TIME '{{ daily_snapshot_time }}') MINUTE),
            TIMESTAMP_ADD(
                TIMESTAMP_ADD(
                    TIMESTAMP_TRUNC({{ end_date }}, DAY),
                INTERVAL EXTRACT(HOUR FROM TIME '{{ daily_snapshot_time }}') HOUR),
            INTERVAL EXTRACT(MINUTE FROM TIME '{{ daily_snapshot_time }}') MINUTE),
        INTERVAL 1 DAY)) AS sdts

    {%- if is_incremental() %}
    WHERE sdts > (SELECT MAX({{ sdts_alias }}) FROM {{ this }})
    {%- endif -%}

),

enriched_timestamps AS (

    SELECT
        sdts as {{ sdts_alias }},
        TRUE as force_active,
        sdts as replacement_sdts,
        CONCAT("Snapshot ", DATE(sdts)) as caption,
        CASE
            WHEN EXTRACT(MINUTE FROM sdts) = 0 AND EXTRACT(SECOND FROM sdts) = 0 THEN TRUE
            ELSE FALSE
        END as is_hourly,
        CASE
            WHEN EXTRACT(MINUTE FROM sdts) = 0 AND EXTRACT(SECOND FROM sdts) = 0 AND EXTRACT(HOUR FROM sdts) = 0 THEN TRUE
            ELSE FALSE
        END as is_daily,
        CASE
            WHEN EXTRACT(DAYOFWEEK FROM sdts) = {{ bigquery_day_of_week_target }} THEN TRUE
            ELSE FALSE
        END as is_beginning_of_week,
        CASE
            WHEN EXTRACT(DAYOFWEEK FROM sdts) = {{ ((bigquery_day_of_week_target + 5) % 7) + 1}} THEN TRUE
            ELSE FALSE
        END as is_end_of_week,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END as is_beginning_of_month,
        CASE 
            WHEN LAST_DAY(DATE(sdts), MONTH) = DATE(sdts) THEN TRUE
            ELSE FALSE
        END as is_end_of_month,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 AND EXTRACT(MONTH from sdts) IN (1,4,7,10) THEN TRUE
            ELSE FALSE
        END AS is_beginning_of_quarter,
        CASE 
            WHEN EXTRACT(MONTH FROM sdts) IN (3, 6, 9, 12) and EXTRACT(DAY FROM DATE_ADD(sdts, INTERVAL 1 DAY)) = 1 THEN TRUE             
            ELSE FALSE 
        END AS is_end_of_quarter,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 AND EXTRACT(MONTH FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END as is_beginning_of_year,
        CASE
            WHEN LAST_DAY(DATE(sdts), YEAR) = DATE(sdts) THEN TRUE
            ELSE FALSE
        END AS is_end_of_year,
        DATE_TRUNC(DATE(sdts), {{ bigquery_day_of_week_arg }}) as beginning_of_week,
        LAST_DAY(DATE(sdts), {{ bigquery_day_of_week_arg }}) as end_of_week,
        DATE_TRUNC(DATE(sdts), MONTH) as beginning_of_month,
        LAST_DAY(DATE(sdts), MONTH) as end_of_month,
        DATE_TRUNC(DATE(sdts), QUARTER) as beginning_of_quarter,
        LAST_DAY(DATE(sdts), QUARTER) as end_of_quarter,
        DATE_TRUNC(DATE(sdts), YEAR) as beginning_of_year,
        LAST_DAY(DATE(sdts), YEAR) as end_of_year,
        NULL as comment
    FROM initial_timestamps

)

SELECT * FROM enriched_timestamps

{%- endmacro -%}
