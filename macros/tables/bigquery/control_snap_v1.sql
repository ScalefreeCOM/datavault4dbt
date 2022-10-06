{%- macro default__control_snap_v1(control_snap_v0, log_logic, sdts_alias) -%}

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


WITH

latest_row AS (

    SELECT
        sdts
    FROM {{ v0_relation }}
    ORDER BY sdts DESC
    LIMIT 1

),

virtual_logic AS (

    SELECT
        c.sdts,
        c.replacement_sdts,

        {%- if log_logic is none %}
        TRUE as is_active,
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

                    (EXTRACT(DATE FROM c.sdts) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {{ daily_duration }} {{ daily_unit }}) AND CURRENT_DATE())
                {%- endif -%}
            {%- endif %}

            {%- if 'weekly' in log_logic.keys() %}
            OR
                {%- if log_logic['weekly']['forever'] == 'TRUE' -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                    (c.is_weekly = TRUE)
                {%- else %}

                    {%- set weekly_duration = log_logic['weekly']['duration'] -%}
                    {%- set weekly_unit = log_logic['weekly']['unit'] -%}

                    (
                (EXTRACT(DATE FROM c.sdts) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {{ weekly_duration }} {{ weekly_unit }}) AND CURRENT_DATE() )
                AND
                (c.is_weekly = TRUE)
            )
                {%- endif -%}
            {% endif -%}

            {%- if 'monthly' in log_logic.keys() %}
            OR
                {%- if log_logic['monthly']['forever'] == 'TRUE' -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                    (c.is_monthly = TRUE)
                {%- else %}

                    {%- set monthly_duration = log_logic['monthly']['duration'] -%}
                    {%- set monthly_unit = log_logic['monthly']['unit'] -%}

                    (
                (EXTRACT(DATE FROM c.sdts) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {{ monthly_duration }} {{ monthly_unit }}) AND CURRENT_DATE() )
                AND
                (c.is_monthly = TRUE)
            )
                {%- endif -%}
            {% endif -%}

            {%- if 'yearly' in log_logic.keys() %}
            OR
                {%- if log_logic['yearly']['forever'] == 'TRUE' -%}
                    {%- set ns.forever_status = 'TRUE' -%}
                    (c.is_yearly = TRUE)
                {%- else %}

                    {%- set yearly_duration = log_logic['yearly']['duration'] -%}
                    {%- set yearly_unit = log_logic['yearly']['unit'] -%}

                    (
                (EXTRACT(DATE FROM c.sdts) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {{ yearly_duration }} {{ yearly_unit }}) AND CURRENT_DATE() )
                AND
                (c.is_yearly = TRUE)
            )
                {%- endif -%}
            {% endif %}
            THEN TRUE
            ELSE FALSE

        END AS is_active,
        {%- endif %}

        CASE
            WHEN l.sdts IS NULL THEN FALSE
            ELSE TRUE
        END AS is_latest,

        c.caption,
        c.is_hourly,
        c.is_daily,
        c.is_weekly,
        c.is_monthly,
        c.is_yearly,
        c.ldts,
        c.comment,
        CASE
            WHEN EXTRACT(YEAR FROM c.sdts) = EXTRACT(YEAR FROM CURRENT_DATE()) THEN TRUE
            ELSE FALSE
        END AS is_current_year,
        CASE
            WHEN EXTRACT(YEAR FROM c.sdts) = EXTRACT(YEAR FROM CURRENT_DATE())-1 THEN TRUE
            ELSE FALSE
        END AS is_last_year,
        CASE
            WHEN EXTRACT(DATE FROM c.sdts) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) AND CURRENT_DATE() THEN TRUE
            ELSE FALSE
        END AS is_rolling_year,
        CASE
            WHEN EXTRACT(DATE FROM c.sdts) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) THEN TRUE
            ELSE FALSE
        END AS is_last_rolling_year
    FROM {{ v0_relation }} c
    LEFT JOIN latest_row l
        ON c.sdts = l.sdts

)

SELECT * FROM virtual_logic

{%- endmacro -%}
