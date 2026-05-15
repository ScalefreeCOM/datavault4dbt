{%- macro trino__control_snap_v0(start_date, daily_snapshot_time, sdts_alias, end_date=none) -%}

{% if datavault4dbt.is_nothing(end_date) %}
  {% set end_date = 'CAST(CURRENT_TIMESTAMP AS ' ~ datavault4dbt.timestamp_default_dtype() ~ ')' %}
{% else %}
    {% set end_date = "CAST('" ~ end_date ~ "' AS " ~ datavault4dbt.timestamp_default_dtype() ~ ") + INTERVAL '1' DAY" %}
{% endif %}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- if not datavault4dbt.is_something(sdts_alias) -%}
    {%- set sdts_alias = var('datavault4dbt.sdts_alias', 'sdts') -%}
{%- endif -%}

{%- set first_day = datavault4dbt.first_day_of_week() -%}
{%- set last_day = 7 if first_day == 1 else first_day - 1 -%}

WITH

initial_timestamps AS (

    SELECT
        sdts
    FROM
        UNNEST(SEQUENCE(CAST('{{ start_date }} {{ daily_snapshot_time }}' AS {{ datavault4dbt.timestamp_default_dtype() }}), {{ end_date }}, INTERVAL '1' DAY)) AS t(sdts)
    {%- if is_incremental() %}
    WHERE
        sdts > (SELECT MAX({{ sdts_alias }}) FROM {{ this }})
    {%- endif %}

),

enriched_timestamps AS (

    SELECT
        sdts as {{ sdts_alias }},
        TRUE as force_active,
        sdts as replacement_sdts,
        CONCAT('Snapshot ', CAST(CAST(sdts AS DATE) AS VARCHAR)) as caption,
        CASE
            WHEN EXTRACT(MINUTE FROM sdts) = 0 AND EXTRACT(SECOND FROM sdts) = 0 THEN TRUE
            ELSE FALSE
        END as is_hourly,
        CASE
            WHEN EXTRACT(MINUTE FROM sdts) = 0 AND EXTRACT(SECOND FROM sdts) = 0 AND EXTRACT(HOUR FROM sdts) = 0 THEN TRUE
            ELSE FALSE
        END as is_daily,
        CASE
            WHEN EXTRACT(DAY_OF_WEEK FROM sdts) = {{ first_day }} THEN TRUE
            ELSE FALSE
        END as is_beginning_of_week,
        CASE
            WHEN EXTRACT(DAY_OF_WEEK FROM sdts) = {{ last_day }} THEN TRUE
            ELSE FALSE
        END as is_end_of_week,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END as is_beginning_of_month,
        CASE
            WHEN CAST(sdts AS DATE) = last_day_of_month(CAST(sdts AS DATE)) THEN TRUE
            ELSE FALSE
        END as is_end_of_month,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 AND EXTRACT(MONTH FROM sdts) IN (1, 4, 7, 10) THEN TRUE
            ELSE FALSE
        END as is_beginning_of_quarter,
        CASE
            WHEN CAST(sdts AS DATE) = last_day_of_month(CAST(sdts AS DATE)) AND EXTRACT(MONTH FROM sdts) IN (3, 6, 9, 12) THEN TRUE
            ELSE FALSE
        END as is_end_of_quarter,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 AND EXTRACT(MONTH FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END as is_beginning_of_year,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 31 AND EXTRACT(MONTH FROM sdts) = 12 THEN TRUE
            ELSE FALSE
        END as is_end_of_year
    FROM initial_timestamps

)

SELECT * FROM enriched_timestamps

{%- endmacro -%}
