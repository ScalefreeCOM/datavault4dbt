{%- macro databricks__control_snap_v0(start_date, daily_snapshot_time, sdts_alias, end_date=none) -%}

{% if datavault4dbt.is_nothing(end_date) %}
    {% set end_date = 'CURRENT_TIMESTAMP' %}
{% else %}
    {% set end_date = "'"~end_date~"'" %}
{% endif %}

{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- if not datavault4dbt.is_something(sdts_alias) -%}
    {%- set sdts_alias = var('datavault4dbt.sdts_alias', 'sdts') -%}
{%- endif -%}

{%- set first_day_of_week_var = var('datavault4dbt.first_day_of_week').get(target.type, 1) | int -%}

{%- set databricks_day_of_week_target = (first_day_of_week_var % 7) + 1 -%}

{%- if first_day_of_week_var == 7 -%}
    {%- set databricks_day_of_week_arg = 'SUNDAY' -%}
{%- else -%}
    {%- set databricks_day_of_week_arg = 'MONDAY' -%}
{%- endif -%}

WITH 

date_array as(
    select sequence(to_timestamp('{{ start_date }} {{ daily_snapshot_time }}'), to_timestamp(to_date({{ end_date }})+1), interval 1 day) AS sdts
),

cte as(
    select explode(sdts) as sdts
    from date_array
),

initial_timestamps AS (
    
    SELECT *
    FROM 
        cte
    WHERE 
        sdts <= to_date({{ end_date }})+1
    {%- if is_incremental() %}
    AND sdts > (SELECT MAX({{ sdts_alias }}) FROM {{ this }})
    {%- endif %}

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
            WHEN dayofweek(sdts) = {{ databricks_day_of_week_target }} THEN TRUE
            ELSE FALSE
        END as is_beginning_of_week,
        CASE
            WHEN dayofweek(sdts) = {{ ((databricks_day_of_week_target + 5) % 7) + 1 }} THEN TRUE
            ELSE FALSE
        END as is_end_of_week,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END as is_beginning_of_month,
        CASE 
            WHEN LAST_DAY(DATE(sdts)) = DATE(sdts) THEN TRUE
            ELSE FALSE
        END as is_end_of_month,
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 AND EXTRACT(MONTH from sdts) IN (1,4,7,10) THEN TRUE
            ELSE FALSE
        END AS is_beginning_of_quarter,
        CASE 
            WHEN MONTH(sdts) IN (3, 6, 9, 12) AND DAY(sdts) = DAY(LAST_DAY(sdts)) THEN TRUE 
            ELSE FALSE 
        END AS is_end_of_quarter, 
        CASE
            WHEN EXTRACT(DAY FROM sdts) = 1 AND EXTRACT(MONTH FROM sdts) = 1 THEN TRUE
            ELSE FALSE
        END as is_beginning_of_year,
        CASE
            WHEN LAST_DAY(DATE(sdts)) = DATE(sdts) AND EXTRACT (MONTH FROM sdts) = 12 THEN TRUE
            ELSE FALSE
        END AS is_end_of_year,
        '' as comment 
    FROM initial_timestamps

)

SELECT * FROM enriched_timestamps

{%- endmacro -%}
