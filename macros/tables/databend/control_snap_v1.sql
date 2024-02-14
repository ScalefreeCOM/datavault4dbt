{%- macro databend__control_snap_v1(control_snap_v0, log_logic, sdts_alias) -%}

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
{%- set cnt = 0 -%}

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
            {% if 'daily' in log_logic.keys() %} {%- if cnt != 0 %} OR {% endif -%}
                {%- set cnt = cnt + 1 -%}
                {%- if log_logic['daily']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                    (1=1)
                {%- else %}

                    {%- set daily_duration = log_logic['daily']['duration'] -%}
                    {%- set daily_unit = log_logic['daily']['unit'] -%}

                    (DATE_TRUNC('DAY', TO_DATE(c.{{ sdts_alias }})) BETWEEN DATE_SUB(TO_DATE(NOW()), INTERVAL {{ daily_duration }}, {{ daily_unit }}) AND TO_DATE(NOW()))
                {%- endif -%}
            {%- endif %}

            {%- if 'weekly' in log_logic.keys() %} {%- if cnt != 0 %} OR {% endif -%}
                {%- set cnt = cnt + 1 -%}
                {%- if log_logic['weekly']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                    (c.is_weekly = TRUE)
                {%- else %}

                    {%- set weekly_duration = log_logic['weekly']['duration'] -%}
                    {%- set weekly_unit = log_logic['weekly']['unit'] -%}

                    (
                (DATE_TRUNC('DAY', TO_DATE(c.{{ sdts_alias }})) BETWEEN DATE_SUB(TO_DATE(NOW()), INTERVAL {{ weekly_duration }}, {{ weekly_unit }}) AND TO_DATE(NOW()))
                AND
                (c.is_weekly = TRUE)
            )
                {%- endif -%}
            {% endif -%}

            {%- if 'monthly' in log_logic.keys() %} {%- if cnt != 0 %} OR {% endif -%}
                {%- set cnt = cnt + 1 -%}
                {%- if log_logic['monthly']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                    (c.is_monthly = TRUE)
                {%- else %}

                    {%- set monthly_duration = log_logic['monthly']['duration'] -%}
                    {%- set monthly_unit = log_logic['monthly']['unit'] -%}

                    (
                (DATE_TRUNC('DAY', TO_DATE(c.{{ sdts_alias }})) BETWEEN DATE_SUB(TO_DATE(NOW()), INTERVAL {{ monthly_duration }}, {{ monthly_unit }}) AND TO_DATE(NOW()))
                AND
                (c.is_monthly = TRUE)
            )
                {%- endif -%}
            {% endif -%}

            {%- if 'end_of_month' in log_logic.keys() %} {%- if cnt != 0 %} OR {% endif -%}
                {%- set cnt = cnt + 1 -%}
                {%- if log_logic['end_of_month']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                    (c.is_end_of_month = TRUE)
                {%- else %}

                    {%- set end_of_month_duration = log_logic['end_of_month']['duration'] -%}
                    {%- set end_of_month_unit = log_logic['end_of_month']['unit'] -%}

                    (
                (DATE_TRUNC('DAY', TO_DATE(c.{{ sdts_alias }})) BETWEEN DATE_SUB(TO_DATE(NOW()), INTERVAL {{ end_of_month_duration }}, {{ end_of_month_unit }}) AND TO_DATE(NOW()))
                AND
                (c.is_end_of_month = TRUE)
            )
                {%- endif -%}
            {% endif -%}

            {%- if 'quarterly' in log_logic.keys() %} {%- if cnt != 0 %} OR {% endif -%}
                {%- set cnt = cnt + 1 -%}
                {%- if log_logic['quarterly']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                    (c.is_quarterly = TRUE)
                {%- else %}

                    {%- set quarterly_duration = log_logic['quarterly']['duration'] -%}
                    {%- set quarterly_unit = log_logic['quarterly']['unit'] -%}

                    (
                (DATE_TRUNC('DAY', TO_DATE(c.{{ sdts_alias }})) BETWEEN DATE_SUB(TO_DATE(NOW()), INTERVAL {{ quarterly_duration }}, {{ quarterly_unit }}) AND TO_DATE(NOW()))
                AND
                (c.is_quarterly = TRUE)
            )
                {%- endif -%}
            {% endif %}

            {%- if 'yearly' in log_logic.keys() %} {%- if cnt != 0 %} OR {% endif -%}
                {%- set cnt = cnt + 1 -%}
                {%- if log_logic['yearly']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                    (c.is_yearly = TRUE)
                {%- else %}

                    {%- set yearly_duration = log_logic['yearly']['duration'] -%}
                    {%- set yearly_unit = log_logic['yearly']['unit'] -%}

                    (
                (DATE_TRUNC('DAY', TO_DATE(c.{{ sdts_alias }})) BETWEEN DATE_SUB(TO_DATE(NOW()), INTERVAL {{ yearly_duration }}, {{ yearly_unit }}) AND TO_DATE(NOW()))
                AND
                (c.is_yearly = TRUE)
            )
                {%- endif -%}
            {% endif %}

            {%- if 'end_of_year' in log_logic.keys() %} {%- if cnt != 0 %} OR {% endif -%}
                {%- set cnt = cnt + 1 -%}
                {%- if log_logic['end_of_year']['forever'] is true -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                    (c.is_end_of_year = TRUE)
                {%- else %}

                    {%- set end_of_year_duration = log_logic['end_of_year']['duration'] -%}
                    {%- set end_of_year_unit = log_logic['end_of_year']['unit'] -%}

                    (
                (DATE_TRUNC('DAY', TO_DATE(c.{{ sdts_alias }})) BETWEEN DATE_SUB(TO_DATE(NOW()), INTERVAL {{ end_of_year_duration }}, {{ end_of_year_unit }}) AND TO_DATE(NOW()))
                AND
                (c.is_end_of_year = TRUE)
            )
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
        c.is_end_of_month,
        c.is_quarterly,
        c.is_yearly,
        c.is_end_of_year,
        CASE
            WHEN EXTRACT(YEAR FROM c.{{ sdts_alias }}) = EXTRACT(YEAR FROM TO_DATE(NOW())) THEN TRUE
            ELSE FALSE
        END AS is_current_year,
        CASE
            WHEN EXTRACT(YEAR FROM c.{{ sdts_alias }}) = EXTRACT(YEAR FROM TO_DATE(NOW()))-1 THEN TRUE
            ELSE FALSE
        END AS is_last_year,
        CASE
            WHEN EXTRACT(DATE FROM c.{{ sdts_alias }}) BETWEEN DATE_SUB(TO_DATE(NOW()), INTERVAL 1, YEAR) AND TO_DATE(NOW()) THEN TRUE
            ELSE FALSE
        END AS is_rolling_year,
        CASE
            WHEN EXTRACT(DATE FROM c.{{ sdts_alias }}) BETWEEN DATE_SUB(TO_DATE(NOW()), INTERVAL 2, YEAR) AND DATE_SUB(TO_DATE(NOW()), INTERVAL 1, YEAR) THEN TRUE
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
        is_end_of_month,
        is_quarterly,
        is_yearly,
        is_end_of_year,
        is_current_year,
        is_last_year,
        is_rolling_year,
        is_last_rolling_year,
        comment
    FROM virtual_logic

)

SELECT * FROM active_logic_combined

    {%- endmacro -%}
