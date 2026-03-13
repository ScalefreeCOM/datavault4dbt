{%- macro snowflake__control_snap_v1(control_snap_v0, log_logic, sdts_alias) -%}

{# Sample intervals
   {%-set log_logic = {'daily': {'duration': 3,
                                'unit': 'MONTH',
                                'forever': false},
                      'weekly': {'duration': 1,
                                 'unit': 'YEAR'},
                      'monthly': {'duration': 5,
                                  'unit': 'YEAR'},
                      'yearly': {'forever': true} } %} 
#}

{%- if log_logic is not none %}
    {%- for interval in log_logic.keys() %}
        {%- if 'forever' not in log_logic[interval].keys() -%}
            {% do log_logic[interval].update({'forever': false}) %}
        {%- endif -%}
    {%- endfor -%}
{%- endif %}

{%- set v0_relation = ref('control_snap_v0') -%}
{%- set ns = namespace(forever_status=FALSE) %}

{%- set snapshot_trigger_column = var('datavault4dbt.snapshot_trigger_column', 'is_active') -%}

WITH 

latest_row AS (

    SELECT {{ sdts_alias }}
    FROM {{ v0_relation }}
    ORDER BY {{ sdts_alias }} DESC
    LIMIT 1

), 

virtual_logic AS (
    
    SELECT
        c.{{ sdts_alias }},
        c.replacement_sdts,
        c.force_active,
        {%- if log_logic is none %}
        TRUE AS {{ snapshot_trigger_column }},
        {%- else %}
            {% if 'daily' in log_logic.keys() %}
                {%- if log_logic['daily']['forever'] -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                    (1=1)
                {%- else %}                            
                    {%- set daily_duration = log_logic['daily']['duration'] -%}
                    {%- set daily_unit = log_logic['daily']['unit'] -%}
                    (DATE_TRUNC('DAY', c.{{ sdts_alias }}::DATE) BETWEEN CURRENT_DATE() - INTERVAL '{{ daily_duration }} {{ daily_unit }}' AND CURRENT_DATE())
                {%- endif -%}   
            {%- endif %}
            {%- if 'weekly' in log_logic.keys() %} 
                {% if log_logic['weekly']['forever'] -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                OR (c.is_weekly = TRUE)
                {%- else %} 
                    {%- set weekly_duration = log_logic['weekly']['duration'] -%}
                    {%- set weekly_unit = log_logic['weekly']['unit'] %}            
                OR ((DATE_TRUNC('DAY', c.{{ sdts_alias }}::DATE) BETWEEN CURRENT_DATE() - INTERVAL '{{ weekly_duration }} {{ weekly_unit }}' AND CURRENT_DATE()) AND (c.is_weekly = TRUE))
                {%- endif -%}
            {% endif -%}
            {%- if 'monthly' in log_logic.keys() %}
                {%- if log_logic['monthly']['forever'] -%}
                    {%- set ns.forever_status = 'TRUE' %}
                OR (c.is_monthly = TRUE)
                {%- else %}
                    {%- set monthly_duration = log_logic['monthly']['duration'] -%}
                    {%- set monthly_unit = log_logic['monthly']['unit'] %}            
                OR ((DATE_TRUNC('DAY', c.{{ sdts_alias }}::DATE) BETWEEN CURRENT_DATE() - INTERVAL '{{ monthly_duration }} {{ monthly_unit }}' AND CURRENT_DATE()) AND (c.is_monthly = TRUE))
                {%- endif -%}
            {% endif -%}
            {%- if 'yearly' in log_logic.keys() %}
                {%- if log_logic['yearly']['forever'] -%}
                    {%- set ns.forever_status = 'TRUE' %}
                OR (c.is_yearly = TRUE)
                {%- else %}
                    {%- set yearly_duration = log_logic['yearly']['duration'] -%}
                    {%- set yearly_unit = log_logic['yearly']['unit'] %}                    
                OR ((DATE_TRUNC('DAY', c.{{ sdts_alias }}::DATE) BETWEEN CURRENT_DATE() - INTERVAL '{{ yearly_duration }} {{ yearly_unit }}' AND CURRENT_DATE()) AND (c.is_yearly = TRUE))
                {%- endif -%}
            {% endif %} AS {{ snapshot_trigger_column }},
        {%- endif %}

        l.{{ sdts_alias }} IS NOT NULL AS is_latest,
        c.caption,
        c.is_hourly,
        c.is_daily,
        c.is_weekly,
        c.is_monthly,
        c.is_yearly,
        EXTRACT(YEAR FROM c.{{ sdts_alias }}) = EXTRACT(YEAR FROM CURRENT_DATE()) AS is_current_year,
        EXTRACT(YEAR FROM c.{{ sdts_alias }}) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1 AS is_last_year,
        DATE_TRUNC('DAY', c.{{ sdts_alias }}::DATE) BETWEEN (CURRENT_DATE() - INTERVAL '1 YEAR') AND CURRENT_DATE() AS is_rolling_year,
        DATE_TRUNC('DAY', c.{{ sdts_alias }}::DATE) BETWEEN (CURRENT_DATE() - INTERVAL '2 YEAR') AND (CURRENT_DATE() - INTERVAL '1 YEAR') AS is_last_rolling_year,
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
            WHEN force_active THEN TRUE
            ELSE {{ snapshot_trigger_column }}
        END AS {{ snapshot_trigger_column }},
        is_latest, 
        caption,
        is_hourly,
        is_daily,
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
