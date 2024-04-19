{%- macro oracle__control_snap_v1(control_snap_v0, log_logic, sdts_alias) -%}

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
{%- set cnt = 0 -%}
WITH

latest_row AS (

    SELECT *
    FROM (
            SELECT
                {{ sdts_alias }}
            FROM {{ v0_relation }}
            ORDER BY {{ sdts_alias }} DESC
          )
    WHERE rownum = 1
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
            {% if 'daily' in log_logic.keys() %} {%- if cnt != 0 %} OR {% endif -%}
                {%- set cnt = cnt + 1 -%}
                {%- if log_logic['daily']['forever'] == 'TRUE' -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                  (1=1)
                {%- else %}
                    {%- set daily_duration = log_logic['daily']['duration'] -%}
                    {%- set daily_unit = log_logic['daily']['unit'] -%}
                  TRUNC(c.{{ sdts_alias }}, 'DD') BETWEEN TRUNC(sysdate) - INTERVAL '{{ daily_duration }}' {{ daily_unit}} AND TRUNC(sysdate)
                {%- endif -%}
            {%- endif %}

            {%- if 'monthly' in log_logic.keys() %} {%- if cnt != 0 %} OR {% endif -%}
                {%- set cnt = cnt + 1 -%}
                {%- if log_logic['weekly']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                    (c.is_weekly = 1)
                {%- else %}

                    {%- set weekly_duration = log_logic['weekly']['duration'] -%}
                    {%- set weekly_unit = log_logic['weekly']['unit'] -%}

                    ((TRUNC(c.{{ sdts_alias }}, 'DD') BETWEEN TRUNC(sysdate) - INTERVAL '{{ weekly_duration }}' {{ weekly_unit}} AND TRUNC(sysdate))
                    AND
                    (c.is_weekly = 1))



                {%- endif -%}
            {% endif -%}

            {%- if 'monthly' in log_logic.keys() %} {%- if cnt != 0 %} OR {% endif -%}
                {%- set cnt = cnt + 1 -%}
                {%- if log_logic['monthly']['forever'] == 'TRUE' -%}
                    {%- set ns.forever_status = 'TRUE' %}
              (c.is_monthly = 1)
                {%- else %}
                    {%- set monthly_duration = log_logic['monthly']['duration'] -%}
                    {%- set monthly_unit = log_logic['monthly']['unit'] %}
                    ((TRUNC(c.{{ sdts_alias }}, 'DD') BETWEEN TRUNC(sysdate) - INTERVAL '{{ monthly_duration }}' {{ monthly_unit}} AND TRUNC(sysdate))
                    AND
                    (c.is_monthly = 1))
                {%- endif -%}
            {% endif -%}

            {%- if 'end_of_month' in log_logic.keys() %} {%- if cnt != 0 %} OR {% endif -%}
                {%- set cnt = cnt + 1 -%}
                {%- if log_logic['end_of_month']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' %}
              (c.is_end_of_month = 1)
                {%- else %}
                    {%- set end_of_month_duration = log_logic['end_of_month']['duration'] -%}
                    {%- set end_of_month_unit = log_logic['end_of_month']['unit'] %}
              ((TRUNC(c.{{ sdts_alias }}, 'DD') BETWEEN TRUNC(sysdate) - INTERVAL '{{ end_of_month_duration }}' {{ end_of_month_unit}} AND TRUNC(sysdate))
                AND (c.is_end_of_month = 1))
                {%- endif -%}
            {% endif -%}

            {%- if 'quarterly' in log_logic.keys() %} {%- if cnt != 0 %} OR {% endif -%}
                {%- set cnt = cnt + 1 -%}
                {%- if log_logic['quarterly']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' %}
              (c.is_quarterly = 1)
                {%- else %}
                    {%- set quarterly_duration = log_logic['quarterly']['duration'] -%}
                    {%- set quarterly_unit = log_logic['quarterly']['unit'] %}
              ((TRUNC(c.{{ sdts_alias }}, 'DD') BETWEEN TRUNC(sysdate) - INTERVAL '{{ quarterly_duration }}' {{ quarterly_unit}} AND TRUNC(sysdate))
              AND (c.is_quarterly = 1))
                {%- endif -%}
            {% endif -%}

            {%- if 'yearly' in log_logic.keys() %} {%- if cnt != 0 %} OR {% endif -%}
                {%- set cnt = cnt + 1 -%}
                {%- if log_logic['yearly']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                    (c.is_yearly = 1)
                {%- else %}
                    {%- set yearly_duration = log_logic['yearly']['duration'] -%}
                    {%- set yearly_unit = log_logic['yearly']['unit'] %}
                    ((TRUNC(c.{{ sdts_alias }}, 'DD') BETWEEN TRUNC(sysdate) - INTERVAL '{{ yearly_duration }}' {{ yearly_unit}} AND TRUNC(sysdate))
                    AND
                    (c.is_yearly = 1))
                {%- endif -%}
            {% endif %}

            {%- if 'end_of_year' in log_logic.keys() %} {%- if cnt != 0 %} OR {% endif -%}
                {%- set cnt = cnt + 1 -%}
                {%- if log_logic['end_of_year']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' %}
              (c.is_end_of_year = 1)
                {%- else %}
                    {%- set end_of_year_duration = log_logic['end_of_year']['duration'] -%}
                    {%- set end_of_year_unit = log_logic['end_of_year']['unit'] %}
              ((TRUNC(c.{{ sdts_alias }}, 'DD') BETWEEN TRUNC(sysdate) - INTERVAL '{{ end_of_year_duration }}' {{ end_of_year_unit}} AND TRUNC(sysdate))
              AND (c.is_end_of_year = 1))
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
        c.is_weekly,
        c.is_monthly,
        c.is_end_of_month,
        c.is_quarterly,
        c.is_yearly,
        c.is_end_of_year,
        CASE
            WHEN EXTRACT(YEAR FROM c.{{ sdts_alias }}) = EXTRACT(YEAR FROM sysdate) THEN 1
            ELSE 0
        END AS is_current_year,
        CASE
            WHEN EXTRACT(YEAR FROM c.{{ sdts_alias }}) = EXTRACT(YEAR FROM sysdate)-1 THEN 1
            ELSE 0
        END AS is_last_year,
        CASE
            WHEN TRUNC(TO_DATE(c.{{ sdts_alias }}), 'DD') BETWEEN ADD_MONTHS(TRUNC(sysdate), -12) AND TRUNC(sysdate) THEN 1
            ELSE 0
        END AS is_rolling_year,
        CASE
            WHEN TRUNC(TO_DATE(c.{{ sdts_alias }}), 'DD') BETWEEN ADD_MONTHS(TRUNC(sysdate), -24) AND ADD_MONTHS(TRUNC(sysdate), -12) THEN 1
            ELSE 0
        END AS is_last_rolling_year,
        c."comment"
    FROM {{ v0_relation }} c
    LEFT JOIN latest_row l
        ON c.{{ sdts_alias }} = l.{{ sdts_alias }}

),

active_logic_combined AS (

    SELECT
        {{ sdts_alias }},
        replacement_sdts,
        CASE
            WHEN force_active = 1 AND {{ snapshot_trigger_column }} = 1  THEN 1
            WHEN force_active = 0 OR NOT {{ snapshot_trigger_column }} = 1 THEN 0
        END AS {{ snapshot_trigger_column }},
        is_latest, 
        caption,
        is_hourly,
        is_daily,
        is_weekly,
        is_monthly,
        is_end_of_month,
        is_quarterly,
        is_yearly,
        is_end_of_year,
        is_current_year,
        is_last_year,
        is_rolling_year,
        is_last_rolling_year,
        "comment"
    FROM virtual_logic

)

SELECT * FROM active_logic_combined

{%- endmacro -%}
