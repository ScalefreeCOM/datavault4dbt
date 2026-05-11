{%- macro redshift__control_snap_v1(control_snap_v0, log_logic, sdts_alias) -%}

{# Sample intervals
    {%-set log_logic = {'daily': {'duration': 3, 'unit': 'mons', 'forever': 'FALSE'},
                       'weekly': {'duration': 1, 'unit': 'yr'},
                       'monthly': {'duration': 5, 'unit': 'yr'},
                       'yearly': {'forever': 'TRUE'} } %}

OR for multiple logics:
log_logic:
    - is_active_1:
        monthly:
            duration: 1
            unit: 'yr'
    - is_active_2:
        weekly:
            duration: 2
            unit: 'mons'
#}

{%- set snapshot_trigger_column = var('datavault4dbt.snapshot_trigger_column', 'is_active') -%}
{%- set ns = namespace(forever_status_dict={}, log_logic_list=[], col_name='', log_logic={}, or_required=False) %}

{%- if log_logic is not none %}

    {%- if log_logic is mapping -%}

        {%- for interval in log_logic.keys() %}
            {%- if 'forever' not in log_logic[interval].keys() -%}
                {% do log_logic[interval].update({'forever': 'FALSE'}) %}
            {%- endif -%}
        {%- endfor -%}

        {%- do ns.log_logic_list.append({snapshot_trigger_column: log_logic}) -%}
        {%- do ns.forever_status_dict.update({snapshot_trigger_column: 'FALSE'}) -%}

    {%- elif datavault4dbt.is_list(log_logic) -%}

        {%- for logic in log_logic -%}

            {{ log('logic: ' ~ logic, false) }}
            {% for col_name, log_logic in logic.items() -%}
                {%- set ns.col_name = col_name -%}
                {%- set ns.log_logic = log_logic %}
            {%- endfor -%}

            {%- for interval in ns.log_logic.keys() %}
                {%- if 'forever' not in ns.log_logic[interval].keys() -%}
                    {% do ns.log_logic[interval].update({'forever': 'FALSE'}) %}
                {%- endif -%}
            {%- endfor -%}

            {%- do ns.log_logic_list.append({ns.col_name: ns.log_logic}) -%}
            {%- do ns.forever_status_dict.update({ns.col_name: 'FALSE'}) -%}

        {%- endfor -%}

    {%- else -%}

        {{ exceptions.raise_compiler_error("Invalid format of log_logic definition in Snapshot Control v1. Either one Dictionary with the config, or a list of dictionaries with the name of the output col as a key, and the log config as each value.")}}

    {%- endif -%}
{%- endif %}

{%- set v0_relation = ref(control_snap_v0) -%}

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
            {% for logic in ns.log_logic_list -%}

                {% set ns.or_required = False %}

                {% for col_name, log_logic in logic.items() -%}
                    {%- set ns.col_name = col_name -%}
                    {%- set ns.log_logic = log_logic %}
                {%- endfor -%}
                {%- set col_name = ns.col_name -%}
                {%- set log_logic = ns.log_logic -%}

                CASE
                    WHEN
                    {% if 'daily' in log_logic.keys() %}
                        {% set ns.or_required = True %}
                        {%- if log_logic['daily']['forever'] is true -%}
                            {%- do ns.forever_status_dict.update({col_name: 'TRUE'}) -%}
                        (1=1)
                        {%- else %}
                            {%- set daily_duration = log_logic['daily']['duration'] -%}
                            {%- set daily_unit = log_logic['daily']['unit'] -%}
                        c.{{ sdts_alias }} BETWEEN dateadd({{ daily_unit }}, -{{ daily_duration }}, GETDATE()) AND GETDATE()
                        {%- endif -%}
                    {%- endif %}

                    {%- if 'weekly' in log_logic.keys() %} {{ 'OR' if ns.or_required is true }}
                        {% set ns.or_required = True %}
                        {%- if log_logic['weekly']['forever'] is true -%}
                            {%- do ns.forever_status_dict.update({col_name: 'TRUE'}) -%}
                    (c.is_beginning_of_week = TRUE)
                        {%- else %}
                            {%- set weekly_duration = log_logic['weekly']['duration'] -%}
                            {%- set weekly_unit = log_logic['weekly']['unit'] %}
                    (c.{{ sdts_alias }} BETWEEN dateadd({{ weekly_unit }}, -{{ weekly_duration }}, GETDATE()) AND GETDATE() AND (c.is_beginning_of_week = TRUE))
                        {%- endif -%}
                    {% endif -%}

                    {%- if 'monthly' in log_logic.keys() %} {{ 'OR' if ns.or_required is true }}
                        {% set ns.or_required = True %}
                        {%- if log_logic['monthly']['forever'] is true -%}
                            {%- do ns.forever_status_dict.update({col_name: 'TRUE'}) -%}
                    (c.is_beginning_of_month = TRUE)
                        {%- else %}
                            {%- set monthly_duration = log_logic['monthly']['duration'] -%}
                            {%- set monthly_unit = log_logic['monthly']['unit'] %}
                    (c.{{ sdts_alias }} BETWEEN dateadd({{ monthly_unit }}, -{{ monthly_duration }}, GETDATE()) AND GETDATE() AND (c.is_beginning_of_month = TRUE))
                        {%- endif -%}
                    {% endif -%}

                    {%- if 'yearly' in log_logic.keys() %} {{ 'OR' if ns.or_required is true }}
                        {% set ns.or_required = True %}
                        {%- if log_logic['yearly']['forever'] is true -%}
                            {%- do ns.forever_status_dict.update({col_name: 'TRUE'}) -%}
                    (c.is_beginning_of_year = TRUE)
                        {%- else %}
                            {%- set yearly_duration = log_logic['yearly']['duration'] -%}
                            {%- set yearly_unit = log_logic['yearly']['unit'] %}
                    (c.{{ sdts_alias }} BETWEEN dateadd({{ yearly_unit }}, -{{ yearly_duration }}, GETDATE()) AND GETDATE() AND (c.is_beginning_of_year = TRUE))
                        {%- endif -%}
                    {% endif %}
                    THEN TRUE
                    ELSE FALSE
                END AS {{ col_name }},
            {% endfor %}
        {%- endif %}

        CASE
            WHEN l.{{ sdts_alias }} IS NULL THEN FALSE
            ELSE TRUE
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
            WHEN EXTRACT(yr FROM c.{{ sdts_alias }}) = EXTRACT(yr FROM CURRENT_DATE) THEN TRUE
            ELSE FALSE
        END AS is_current_year,
        CASE
            WHEN EXTRACT(yr FROM c.{{ sdts_alias }}) = EXTRACT(yr FROM CURRENT_DATE)- '1' THEN TRUE
            ELSE FALSE
        END AS is_last_year,
        CASE
            WHEN c.{{ sdts_alias }} BETWEEN dateadd(Year, -1, GETDATE()) AND CURRENT_DATE THEN TRUE
            ELSE FALSE
        END AS is_rolling_year,
        CASE
            WHEN c.{{ sdts_alias }} BETWEEN dateadd(Year, -2, GETDATE()) AND dateadd(Year, -1, GETDATE()) THEN TRUE
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
        {%- if log_logic is none %}
            CASE
                WHEN force_active AND {{ snapshot_trigger_column }} THEN TRUE
                WHEN NOT force_active OR NOT {{ snapshot_trigger_column }} THEN FALSE
            END AS {{ snapshot_trigger_column }},
        {%- else %}
            {%- for logic in ns.log_logic_list %}
                {% for col_name, log_logic in logic.items() -%}
                    {{ col_name }},
                {%- endfor -%}
            {% endfor %}
        {%- endif %}
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
        comment
    FROM virtual_logic

)

SELECT * FROM active_logic_combined

{%- endmacro -%}