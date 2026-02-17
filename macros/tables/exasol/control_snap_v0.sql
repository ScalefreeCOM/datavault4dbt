{%- macro exasol__control_snap_v0(start_date, daily_snapshot_time, sdts_alias, end_date=none) -%}


{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{% if datavault4dbt.is_nothing(end_date) %}
  {% set end_date = 'CURRENT_DATE()' %}
{% else %}
    {% set end_date = datavault4dbt.string_to_timestamp(timestamp_format, end_date) %}
{% endif %}

{%- set date_format_std = 'YYYY-mm-dd' -%}
{%- set daily_snapshot_time = '0001-01-01 ' ~ daily_snapshot_time -%}
{%- set last_cte = '' -%}

{%- set first_day_of_week_var = var('datavault4dbt.first_day_of_week').get(target.type, 1) | int -%}

WITH 
initial_timestamps AS 
(
    select
    add_days(ADD_MINUTES(ADD_HOURS(DATE_TRUNC('day', DATE '{{ start_date }}' ), EXTRACT(HOUR FROM {{ datavault4dbt.string_to_timestamp(timestamp_format, daily_snapshot_time) }}) ),
                                                        EXTRACT(MINUTE FROM  {{ datavault4dbt.string_to_timestamp(timestamp_format, daily_snapshot_time) }}) 
                                                    ), level-1) as sdts
    from dual
    connect by level <= days_between(ADD_MINUTES(ADD_HOURS({{ end_date }}, EXTRACT(HOUR FROM {{ datavault4dbt.string_to_timestamp(timestamp_format, daily_snapshot_time) }}) ),
                                                        EXTRACT(MINUTE FROM  {{ datavault4dbt.string_to_timestamp(timestamp_format, daily_snapshot_time) }}) 
                                                    ), TO_DATE('{{ start_date}}', '{{ date_format_std }}')
                                    )+1
    order by local.sdts

    {%- set last_cte = 'initial_timestamps' -%}
)


{%- if is_incremental() %}
, incremental_cte AS (
    SELECT 
        src.* 
    FROM initial_timestamps src

    WHERE src.sdts > (SELECT MAX(t.{{ sdts_alias }}) FROM {{ this }} t)
    {%- set last_cte = 'incremental_cte' -%}

)
{%- endif %}

, enriched_timestamps AS 
(
    SELECT
        sdts as {{ sdts_alias }},
        TRUE as force_active,
        sdts AS replacement_sdts,
        CONCAT('Snapshot ', TO_CHAR(sdts, 'YYYY-MM-DD')) AS caption,
        CASE
            WHEN EXTRACT(MINUTE FROM sdts) = 0 AND EXTRACT(SECOND FROM sdts) = 0 THEN TRUE
            ELSE FALSE
        END AS is_hourly,
        CASE
            WHEN EXTRACT(MINUTE FROM sdts) = 0 AND EXTRACT(SECOND FROM sdts) = 0 AND EXTRACT(HOUR FROM sdts) = 0 THEN TRUE
            ELSE FALSE
        END AS is_daily,
        CASE 
            WHEN TO_NUMBER(TO_CHAR(sdts, 'ID')) = {{ first_day_of_week_var }} THEN TRUE
            ELSE FALSE
        END AS is_beginning_of_week,
        CASE 
            WHEN TO_NUMBER(TO_CHAR(sdts, 'ID')) = {{ ((first_day_of_week_var + 5) % 7) + 1 }} THEN TRUE
            ELSE FALSE
        END AS is_end_of_week, 
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END AS is_beginning_of_month,
        CASE 
            WHEN LAST_DAY(sdts) = CAST(sdts AS DATE) THEN TRUE
            ELSE FALSE
        END as is_end_of_month,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 AND EXTRACT(MONTH from sdts) IN (1,4,7,10) THEN TRUE
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
            WHEN LAST_DAY(sdts) = CAST(sdts AS DATE) AND EXTRACT (MONTH FROM sdts) = 12 THEN TRUE
            ELSE FALSE
        END AS is_end_of_year,
        {%- if first_day_of_week_var == 7 %}
            CAST(ADD_DAYS(TRUNC(ADD_DAYS(sdts, 1), 'IW'), -1) AS DATE) as beginning_of_week,
            CAST(ADD_DAYS(TRUNC(ADD_DAYS(sdts, 1), 'IW'), 5) AS DATE) as end_of_week,
        {%- else %}
            CAST(TRUNC(sdts, 'IW') AS DATE) as beginning_of_week,
            CAST(ADD_DAYS(TRUNC(sdts, 'IW'), 6) AS DATE) as end_of_week,
        {%- endif %}
        CAST(TRUNC(sdts, 'MM') AS DATE) as beginning_of_month,
        CAST(LAST_DAY(sdts) AS DATE) as end_of_month,
        CAST(TRUNC(sdts, 'Q') AS DATE) as beginning_of_quarter,
        CAST(ADD_DAYS(ADD_MONTHS(TRUNC(sdts, 'Q'), 3), -1) AS DATE) as end_of_quarter,
        CAST(TRUNC(sdts, 'YYYY') AS DATE) as beginning_of_year,
        CAST(ADD_DAYS(ADD_MONTHS(TRUNC(sdts, 'YYYY'), 12), -1) AS DATE) as end_of_year,
        NULL AS comment
    FROM 
        {{ last_cte }}
)

SELECT * FROM enriched_timestamps

{%- endmacro -%}
