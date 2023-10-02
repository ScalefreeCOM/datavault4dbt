{%- macro exasol__control_snap_v1(control_snap_v0, log_logic, sdts_alias) -%}

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
    LIMIT 1

),

virtual_logic AS (

    SELECT
        c.{{ sdts_alias }},
        c.replacement_sdts,
        c.force_active,
        {%- if log_logic is none %}
        TRUE as {{ snapshot_trigger_column }},
        {%- else %}
        CASE 
            WHEN
            {% if 'daily' in log_logic.keys() %}
                {%- if log_logic['daily']['forever'] == 'TRUE' -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                  (1=1)
                {%- else %}                            
                    {%- set daily_duration = log_logic['daily']['duration'] -%}
                    {%- set daily_unit = log_logic['daily']['unit'] -%}
                  (DATE_TRUNC('DAY', TO_DATE(c.{{ sdts_alias }})) BETWEEN ADD_{{ daily_unit}}S(CURRENT_DATE, -{{ daily_duration }}) AND CURRENT_DATE)
                {%- endif -%}   
            {%- endif %}

            {%- if 'monthly' in log_logic.keys() %}
            OR
                {%- if log_logic['weekly']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                    (c.is_weekly = TRUE)
                {%- else %}

                    {%- set weekly_duration = log_logic['weekly']['duration'] -%}
                    {%- set weekly_unit = log_logic['weekly']['unit'] -%}

                    ((DATE_TRUNC('DAY', TO_DATE(c.{{ sdts_alias }})) BETWEEN ADD_{{ weekly_unit}}S(CURRENT_DATE, -{{ weekly_duration }}) AND CURRENT_DATE)
                    AND
                    (c.is_weekly = TRUE))
                {%- endif -%}
            {% endif -%}

            {%- if 'monthly' in log_logic.keys() %} OR
                {%- if log_logic['monthly']['forever'] == 'TRUE' -%}
                    {%- set ns.forever_status = 'TRUE' %}
              (c.is_monthly = TRUE)
                {%- else %}
                    {%- set monthly_duration = log_logic['monthly']['duration'] -%}
                    {%- set monthly_unit = log_logic['monthly']['unit'] %}

                    ((DATE_TRUNC('DAY', TO_DATE(c.{{ sdts_alias }})) BETWEEN ADD_{{ monthly_unit }}S(CURRENT_DATE, -{{ monthly_duration }}) AND CURRENT_DATE) 
                    AND 
                    (c.is_monthly = TRUE))
                {%- endif -%}
            {% endif -%}

            {%- if 'yearly' in log_logic.keys() %}
            OR
                {%- if log_logic['yearly']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                    (c.is_yearly = TRUE)
                {%- else %}
                    {%- set yearly_duration = log_logic['yearly']['duration'] -%}
                    {%- set yearly_unit = log_logic['yearly']['unit'] %}

                    ((DATE_TRUNC('DAY', TO_DATE(c.{{ sdts_alias }})) BETWEEN ADD_{{ yearly_unit }}S(CURRENT_DATE, - {{ yearly_duration }}) AND CURRENT_DATE) 
                    AND 
                    (c.is_yearly = TRUE))
                {%- endif -%}
            {% endif %}
            THEN TRUE
            ELSE FALSE

        END AS {{ snapshot_trigger_column }},
        {%- endif %}
        CASE
            WHEN l.{{ sdts_alias }} IS NULL THEN FALSE
            ELSE TRUE
        END AS is_latest,

        c.caption,
        c.is_hourly,
        c.is_daily,
        c.is_weekly,
        c.is_monthly,
        c.is_yearly,
        CASE
            WHEN EXTRACT(YEAR FROM c.{{ sdts_alias }}) = EXTRACT(YEAR FROM CURRENT_DATE) THEN TRUE
            ELSE FALSE
        END AS is_current_year,
        CASE
            WHEN EXTRACT(YEAR FROM c.{{ sdts_alias }}) = EXTRACT(YEAR FROM CURRENT_DATE)-1 THEN TRUE
            ELSE FALSE
        END AS is_last_year,
        CASE
            WHEN DATE_TRUNC('DAY', TO_DATE(c.{{ sdts_alias }})) BETWEEN ADD_YEARS(CURRENT_DATE,-1) AND CURRENT_DATE THEN TRUE
            ELSE FALSE
        END AS is_rolling_year,
        CASE
            WHEN DATE_TRUNC('DAY', TO_DATE(c.{{ sdts_alias }})) BETWEEN ADD_YEARS(CURRENT_DATE,-2) AND ADD_YEARS(CURRENT_DATE,-1) THEN TRUE
            ELSE FALSE
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
            WHEN force_active AND {{ snapshot_trigger_column }} THEN TRUE
            WHEN NOT force_active OR NOT {{ snapshot_trigger_column }} THEN FALSE
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
