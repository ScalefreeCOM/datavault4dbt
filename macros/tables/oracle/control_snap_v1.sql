{%- macro oracle__control_snap_v1(control_snap_v0, log_logic, sdts_alias) -%}

{# Sample intervals
   {%-set log_logic = {'daily': {'duration': 3,
                                'unit': 'MONTH',
                                'forever': 'FALSE'},
                      'weekly': {'duration': 1,
                                 'unit': 'YEAR'},
                      'monthly': {'duration': 5,
                                  'unit': 'YEAR'},
                      'yearly': {'forever': 'TRUE'} } %} 
#}

{%- if log_logic is not none %}
    {%- for interval in log_logic.keys() %}
        {%- if 'forever' not in log_logic[interval].keys() -%}
            {% do log_logic[interval].update({'forever': 'FALSE'}) %}
        {%- endif -%}
    {%- endfor -%}
{%- endif %}

{%- set v0_relation = ref(control_snap_v0) -%}
{%- set ns = namespace(forever_status=FALSE) %}

{%- set snapshot_trigger_column = var('datavault4dbt.snapshot_trigger_column', 'is_active') -%}

WITH 

latest_row AS (

    SELECT
        {{ sdts_alias }}
    FROM {{ v0_relation }}
    ORDER BY {{ sdts_alias }} DESC
    FETCH FIRST ROW ONLY

), 

virtual_logic AS (
    
    SELECT
        c.{{ sdts_alias }},
        c.replacement_sdts,
        c.force_active,
        {%- if log_logic is none %}
        1 AS {{ snapshot_trigger_column }},
        {%- else %}
        CASE 
            WHEN
            {% if 'daily' in log_logic.keys() %}
                {%- if log_logic['daily']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                  (1=1)
                {%- else %}                             
                    {%- set daily_duration = log_logic['daily']['duration'] -%}
                    {%- set daily_unit = log_logic['daily']['unit'] -%}
                    {# Oracle doesn't work with INTERVAL 'WEEK' #}
                        {% if daily_unit == 'WEEK' %}
                        {% set daily_duration = daily_duration * 7 %}
                        {% set daily_unit = 'DAY' %}
                        {% endif %}    
                  TRUNC(c.{{ sdts_alias }},'DAY') BETWEEN CURRENT_DATE - INTERVAL '{{ daily_duration }}' {{ daily_unit }} AND CURRENT_DATE
                {%- endif -%}   
            {%- endif %}

            {%- if 'weekly' in log_logic.keys() %} OR 
                {%- if log_logic['weekly']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' -%}
              (c.is_beginning_of_week = 1)
                {%- else %} 
                    {%- set weekly_duration = log_logic['weekly']['duration'] -%}
                    {%- set weekly_unit = log_logic['weekly']['unit'] %}
                    {# Oracle doesn't work with INTERVAL 'WEEK' #}
                        {% if weekly_unit == 'WEEK' %}
                        {% set weekly_duration = weekly_duration * 7 %}
                        {% set weekly_unit = 'DAY' %}
                        {% endif %}                    
                    (TRUNC(c.{{ sdts_alias }},'DAY') BETWEEN CURRENT_DATE - INTERVAL '{{ weekly_duration }}' {{ weekly_unit }} AND CURRENT_DATE) AND (c.is_beginning_of_week = 1)
                {%- endif -%}
            {% endif -%}

            {%- if 'monthly' in log_logic.keys() %} OR
                {%- if log_logic['monthly']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' %}
              (c.is_beginning_of_month = 1)
                {%- else %}
                    {%- set monthly_duration = log_logic['monthly']['duration'] -%}
                    {%- set monthly_unit = log_logic['monthly']['unit'] %}
                    {# Oracle doesn't work with INTERVAL 'WEEK' #}
                        {% if monthly_unit == 'WEEK' %}
                        {% set monthly_duration = monthly_duration * 7 %}
                        {% set monthly_unit = 'DAY' %}
                        {% endif %}          
              ((TRUNC(c.{{ sdts_alias }},'DAY') BETWEEN CURRENT_DATE - INTERVAL '{{ monthly_duration }}' {{ monthly_unit }} AND CURRENT_DATE) AND (c.is_beginning_of_month = 1))
                {%- endif -%}
            {% endif -%}

            {%- if 'yearly' in log_logic.keys() %} OR 
                {%- if log_logic['yearly']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' %}
              (c.is_beginning_of_year = 1)
                {%- else %}
                    {%- set yearly_duration = log_logic['yearly']['duration'] -%}
                    {%- set yearly_unit = log_logic['yearly']['unit'] %}
                    {# Oracle doesn't work with INTERVAL 'WEEK' #}
                        {% if yearly_unit == 'WEEK' %}
                        {% set yearly_duration = yearly_duration * 7 %}
                        {% set yearly_unit = 'DAY' %}
                        {% endif %}                   
              ((TRUNC(c.{{ sdts_alias }},'DAY') BETWEEN CURRENT_DATE - INTERVAL '{{ yearly_duration }}' {{ yearly_unit }} AND CURRENT_DATE) AND (c.is_beginning_of_year = 1))
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
        c.is_hourly,
        c.is_daily,
        c.is_beginning_of_week,
        c.is_end_of_week,
        c.is_beginning_of_month,
        c.is_end_of_month,
        c.is_beginning_of_quarter,
        c.is_end_of_quarter,
        c.is_beginning_of_year,
        c.is_end_of_year,
        CASE
            WHEN EXTRACT(YEAR FROM c.{{ sdts_alias }}) = EXTRACT(YEAR FROM CURRENT_DATE) THEN 1
            ELSE 0
        END AS is_current_year,
        CASE
            WHEN EXTRACT(YEAR FROM c.{{ sdts_alias }}) = EXTRACT(YEAR FROM CURRENT_DATE)-1 THEN 1
            ELSE 0
        END AS is_last_year,
        CASE
            WHEN TRUNC(c.{{ sdts_alias }},'DAY') BETWEEN (CURRENT_DATE - INTERVAL '1' YEAR) AND CURRENT_DATE THEN 1
            ELSE 0
        END AS is_rolling_year,
        CASE
            WHEN TRUNC(c.{{ sdts_alias }},'DAY') BETWEEN (CURRENT_DATE - INTERVAL '2' YEAR) AND (CURRENT_DATE - INTERVAL '1' YEAR) THEN 1
            ELSE 0
        END AS is_last_rolling_year,
        c.comment_text
    FROM {{ v0_relation }} c
    LEFT JOIN latest_row l
    ON c.{{ sdts_alias }} = l.{{ sdts_alias }}
),

active_logic_combined AS (

    SELECT 
        {{ sdts_alias }},
        replacement_sdts,
        CASE
            WHEN force_active = 1 AND {{ snapshot_trigger_column }} = 1 THEN 1
            WHEN force_active = 1  OR {{ snapshot_trigger_column }} = 0 THEN 0
        END AS {{ snapshot_trigger_column }},
        is_latest, 
        caption,
        is_hourly,
        is_daily,
        is_beginning_of_week,
        is_end_of_week,
        is_beginning_of_month,
        is_end_of_month,
        is_beginning_of_quarter,
        is_end_of_quarter,
        is_beginning_of_year,
        is_end_of_year,
        is_current_year,
        is_last_year,
        is_rolling_year,
        is_last_rolling_year,
        comment_text
    FROM virtual_logic

)

SELECT * FROM active_logic_combined

{%- endmacro -%}