{%- macro sqlserver__control_snap_v1(control_snap_v0, log_logic, sdts_alias) -%}

{# sample log_logic 
   {%-set log_logic = {'daily': {'duration': 3,
                                'unit': 'MONTH',
                                'forever': 'FALSE'},
                      'monthly': {'duration': 5,
                                  'unit': 'YEAR'},
                      'yearly': {'duration': 10,
                                'unit': 'YEAR'} } %} 

#}

{%- if log_logic is not none %}
    {%- for interval in log_logic.keys() %}
        {%- if 'forever' not in log_logic[interval].keys() -%}
            {% do log_logic[interval].update({'forever': false}) %}
        {%- endif -%}
    {%- endfor -%}
{%- endif %}

{%- set v0_relation = ref(control_snap_v0) -%}
{%- set ns = namespace(forever_status=FALSE) %}

{%- set snapshot_trigger_column = var('datavault4dbt.snapshot_trigger_column', 'is_active') -%}
{%- set cnt = 0 -%}

WITH

latest_row AS (

    SELECT
        TOP (1)
        {{ sdts_alias }}
    FROM {{ v0_relation }}
    ORDER BY {{ sdts_alias }} DESC
    

),

virtual_logic AS (

    SELECT
        c.{{ sdts_alias }},
        c.replacement_sdts,
        c.force_active,
        {%- if log_logic is none %}
        1 as {{ snapshot_trigger_column }},
        {%- else %}
        CASE 
            WHEN
            {% if 'daily' in log_logic.keys() %}
                {%- set cnt = cnt + 1 -%}
                {%- if log_logic['daily']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                  (1=1)
                {%- else %}                            
                    {%- set daily_duration = log_logic['daily']['duration'] -%}
                    {%- set daily_unit = log_logic['daily']['unit'] -%}
                  (DATETRUNC(DAY, convert(date,c.{{ sdts_alias }})) BETWEEN DATEADD({{ daily_unit}}, -{{ daily_duration }},convert(date,SYSDATETIME())) AND convert(date,SYSDATETIME()))
                {%- endif -%}   
            {%- endif %}

            {%- if 'weekly' in log_logic.keys() %} {%- if cnt != 0 %} OR {% endif -%}
                {%- set cnt = cnt + 1 -%}
                {%- if log_logic['weekly']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                    (c.is_end_of_week = 1)
                {%- else %}

                    {%- set weekly_duration = log_logic['weekly']['duration'] -%}
                    {%- set weekly_unit = log_logic['weekly']['unit'] -%}

                    ((DATETRUNC(DAY, convert(date,c.{{ sdts_alias }})) BETWEEN DATEADD({{ weekly_unit}}, -{{ weekly_duration }},convert(date,SYSDATETIME())) AND convert(date,SYSDATETIME()))
                    AND
                    (c.is_end_of_week = 1))
                {%- endif -%}
            {% endif -%}

            {%- if 'monthly' in log_logic.keys() %} {%- if cnt != 0 %} OR {% endif -%}
                {%- set cnt = cnt + 1 -%}
                {%- if log_logic['monthly']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' %}
                (c.is_end_of_month = 1)
                {%- else %}
                    {%- set monthly_duration = log_logic['monthly']['duration'] -%}
                    {%- set monthly_unit = log_logic['monthly']['unit'] %}

                    ((CONVERT(DATE,c.{{ sdts_alias }}) BETWEEN DATEADD({{ monthly_unit}}, -{{ monthly_duration }},convert(date,SYSDATETIME())) AND convert(date,SYSDATETIME()))
                    AND 
                    (c.is_end_of_month = 1))
                {%- endif -%}
            {% endif -%}

            {%- if 'yearly' in log_logic.keys() %} {%- if cnt != 0 %} OR {% endif -%}
                {%- set cnt = cnt + 1 -%}
                {%- if log_logic['yearly']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                    (c.is_end_of_year = 1)
                {%- else %}
                    {%- set yearly_duration = log_logic['yearly']['duration'] -%}
                    {%- set yearly_unit = log_logic['yearly']['unit'] %}

                    ((CONVERT(DATE,c.{{ sdts_alias }}) BETWEEN DATEADD({{ yearly_unit}}, -{{ yearly_duration }},convert(date,SYSDATETIME())) AND convert(date,SYSDATETIME()))
                    AND 
                    (c.is_end_of_year = 1))
                {%- endif -%}
            {% endif %}
            THEN 1
            ELSE 0

        END AS {{ snapshot_trigger_column }},
        {%- endif %}
        CASE
            WHEN l.{{ sdts_alias }} IS NULL THEN 0
            ELSE 1
        END AS is_latest,

        c.caption,
--        c.is_hourly,
--        c.is_daily,
        c.is_end_of_week as is_weekly,
        c.is_end_of_month as is_monthly,
        c.is_end_of_year as is_yearly,
        CASE
            WHEN c.[year] = DATEPART(YEAR,SYSDATETIME()) THEN 1
            ELSE 0
        END AS is_current_year,
        CASE
            WHEN c.[year] = DATEPART(YEAR,SYSDATETIME())-1 THEN 1
            ELSE 0
        END AS is_last_year,
        CASE
            WHEN CONVERT(DATE,c.{{ sdts_alias }}) BETWEEN DATEADD(YEAR,-1,CONVERT(DATE,SYSDATETIME())) AND CONVERT(DATE,SYSDATETIME()) THEN 1
            ELSE 0
        END AS is_rolling_year,
        CASE
            WHEN CONVERT(DATE,c.{{ sdts_alias }}) BETWEEN DATEADD(YEAR,-2,CONVERT(DATE,SYSDATETIME())) AND DATEADD(YEAR,-1,CONVERT(DATE,SYSDATETIME())) THEN 1
            ELSE 0
        END AS is_last_rolling_year,
        c.comment
    FROM {{ v0_relation }} c
    LEFT JOIN latest_row l
        ON c.{{ sdts_alias }} = l.{{ sdts_alias }}

),

active_logic_combined AS (

    SELECT 
        {{ sdts_alias }},
        replacement_sdts,
        CASE
            WHEN (force_active = 1) AND ({{ snapshot_trigger_column }} = 1) THEN 1
            WHEN (NOT force_active = 1) OR (NOT {{ snapshot_trigger_column }} = 1) THEN 0
        END AS {{ snapshot_trigger_column }},
        is_latest, 
        caption,
  --      is_hourly,
  --      is_daily,
        is_weekly,
        is_monthly,
        is_yearly,
        is_current_year,
        is_last_year,
        is_rolling_year,
        is_last_rolling_year,
        comment
    FROM virtual_logic

)

SELECT * FROM active_logic_combined

{%- endmacro -%}
