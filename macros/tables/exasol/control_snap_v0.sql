{%- macro exasol__control_snap_v0(start_date, daily_snapshot_time, sdts_alias) -%}

{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- set date_format_std = 'YYYY-mm-dd' -%}
{%- set daily_snapshot_time = '0001-01-01 ' ~ daily_snapshot_time -%}

WITH 
initial_timestamps AS 
(
    select
    add_days(ADD_MINUTES(ADD_HOURS(DATE_TRUNC('day', DATE '{{ start_date }}' ), EXTRACT(HOUR FROM {{ datavault4dbt.string_to_timestamp(timestamp_format, daily_snapshot_time) }}) ),
                                                        EXTRACT(MINUTE FROM  {{ datavault4dbt.string_to_timestamp(timestamp_format, daily_snapshot_time) }}) 
                                                    ), level-1) as sdts
    from dual
    connect by level <= days_between(ADD_MINUTES(ADD_HOURS(CURRENT_DATE(), EXTRACT(HOUR FROM {{ datavault4dbt.string_to_timestamp(timestamp_format, daily_snapshot_time) }}) ),
                                                        EXTRACT(MINUTE FROM  {{ datavault4dbt.string_to_timestamp(timestamp_format, daily_snapshot_time) }}) 
                                                    ), TO_DATE('{{ start_date}}', '{{ date_format_std }}')
                                    )+1
    {%- if is_incremental() %}
    WHERE sdts > (SELECT MAX({{ sdts_alias }}) FROM {{ this }})
    {%- endif %}
    order by local.sdts
)
, enriched_timestamps AS 
(
    SELECT
        sdts as "{{ sdts_alias }}",
        TRUE as force_active,
        sdts AS replacement_sdts,
        CONCAT('Snapshot ', DATE_TRUNC('day', sdts)) AS caption,
        CASE
            WHEN EXTRACT(MINUTE FROM sdts) = 0 AND EXTRACT(SECOND FROM sdts) = 0 THEN TRUE
            ELSE FALSE
        END AS is_hourly,
        CASE
            WHEN EXTRACT(MINUTE FROM sdts) = 0 AND EXTRACT(SECOND FROM sdts) = 0 AND EXTRACT(HOUR FROM sdts) = 0 THEN TRUE
            ELSE FALSE
        END AS is_daily,
        CASE
            WHEN TO_CHAR(sdts, 'DAY', 'NLS_DATE_LANGUAGE=ENG') = 'MONDAY' THEN TRUE
            ELSE FALSE
        END AS is_weekly,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END AS is_monthly,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 AND EXTRACT(MONTH FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END AS is_yearly,
        NULL AS comment
    FROM 
        initial_timestamps
)
SELECT 
  * 
FROM 
  enriched_timestamps

{%- endmacro -%}
