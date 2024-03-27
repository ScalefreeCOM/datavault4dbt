{%- macro synapse__control_snap_v1(control_snap_v0, log_logic, sdts_alias) -%}

{%- set snapshot_trigger_column = var('datavault4dbt.snapshot_trigger_column', 'is_active') -%}

{%- set ns = namespace(forever_status_dict={}, log_logic_list=[], col_name='', log_logic={}) %}



{%- if log_logic is not none %}

{{ log('log_logic: ' ~ log_logic, false) }}

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
			{% for col_name, logic_definition in logic.items() -%}
				{{ log('logic_definition: ' ~ logic_definition, false) }}
				{{ log('col_name: ' ~ col_name, false) }}
				{%- set ns.col_name = col_name -%}
				{%- set ns.logic_definition = logic_definition %}
			{%- endfor -%}

			{%- for interval in ns.logic_definition.keys() %}
				{%- if 'forever' not in ns.logic_definition[interval].keys() -%}
					{% do ns.logic_definition[interval].update({'forever': 'FALSE'}) %}
				{%- endif -%}
			{%- endfor -%}

			{%- do ns.log_logic_list.append({ns.col_name: ns.logic_definition}) -%}
			{%- do ns.forever_status_dict.update({ns.col_name: 'FALSE'}) -%}

		{%- endfor -%}

	{%- else -%}

		{{ exceptions.raise_compiler_error("Invalid format of log_logic definition in Snapshot Control v1. Either one Dictionary with the config, or a list of dictionaries with the name of the output col as a key, and the log config as each value.")}}

	{%- endif -%}

{%- endif %}

{%- set v0_relation = ref(control_snap_v0) -%}


WITH in_the_past as (

SELECT 
	*,
	ROW_NUMBER() OVER (ORDER BY {{ sdts_alias }} desc) as rn

FROM {{ ref(control_snap_v0) }}
WHERE CONVERT(DATE, {{ sdts_alias }}) <= CONVERT(DATE, GETDATE() )
),

dynamic as (SELECT 
	src.{{ sdts_alias }},
	src.{{ sdts_alias }}_date,
	src.force_active,
	CASE WHEN itp.{{ sdts_alias }} is not null THEN 1 ELSE 0 END AS is_in_the_past,
	CASE WHEN itp.rn = 1 THEN 1 ELSE 0 END AS is_current, 
	CASE WHEN src.year = DATEPART(YEAR, GETDATE()) THEN 1 ELSE 0 END as is_current_year, 
	CASE WHEN src.year = DATEPART(YEAR, GETDATE())-1 THEN 1 ELSE 0 END as is_last_year, 
	CASE WHEN DATEDIFF(day, src.{{ sdts_alias }}, GETDATE()) between 0 and 365 THEN 1 ELSE 0 END as is_current_rolling_year,
	CASE WHEN DATEDIFF(day, src.{{ sdts_alias }}, GETDATE()) between 366 and 730 THEN 1 ELSE 0 END as is_last_rolling_year,
	src.year,
	src.quarter,
	src.month,
	src.day_of_month,
	src.day_of_year,
	src.weekday,
	src.week,
	src.iso_week,
	src.is_end_of_week,
	src.is_end_of_month,
	src.is_end_of_quarter,
	src.is_end_of_year



FROM {{ ref(control_snap_v0) }} src
INNER JOIN in_the_past itp ON src.{{ sdts_alias }} = itp.{{ sdts_alias }}),

log_logic AS (

	SELECT

	*,

	{%- if log_logic is none %}
		1 AS {{ snapshot_trigger_column }},
	{% else %}
		{% for logic in ns.log_logic_list -%}

			{% for col_name, logic_definition in logic.items() -%}
				{{ log('logic_definition: ' ~ logic_definition, false) }}
				{{ log('col_name: ' ~ col_name, false) }}
				{%- set ns.col_name = col_name -%}
				{%- set ns.logic_definition = logic_definition %}
			{%- endfor -%}
			{%- set col_name = ns.col_name -%}
			{{ log('col_name: ' ~ col_name, false) }}
			{%- set logic_definition = ns.logic_definition -%}

			CASE 
				WHEN
				{% if 'daily' in logic_definition.keys() %}
					{%- if logic_definition['daily']['forever'] is true -%}
						{%- do ns.forever_status_dict.update({col_name: 'TRUE'}) -%}
					(1=1)
					{%- else %}                            
						{%- set daily_duration = logic_definition['daily']['duration'] -%}
						{%- set daily_unit = logic_definition['daily']['unit'] -%}
					(c.{{ sdts_alias }} BETWEEN DATEADD({{ daily_unit }}, -{{ daily_duration }}, GETDATE()) AND GETDATE())
					{%- endif -%}   
				{%- endif %}

				{%- if 'weekly' in logic_definition.keys() %} OR 
					{%- if logic_definition['weekly']['forever'] is true -%}
						{%- do ns.forever_status_dict.update({col_name: 'TRUE'}) -%}
				(c.is_end_of_week = 1)
					{%- else %} 
						{%- set weekly_duration = logic_definition['weekly']['duration'] -%}
						{%- set weekly_unit = logic_definition['weekly']['unit'] %}            
				((c.{{ sdts_alias }} BETWEEN DATEADD({{ weekly_unit }}, -{{ weekly_duration }}, GETDATE()) AND GETDATE()) AND (c.is_end_of_week = 1))
					{%- endif -%}
				{% endif -%}

				{%- if 'monthly' in logic_definition.keys() %} OR
					{%- if logic_definition['monthly']['forever'] is true -%}
						{%- do ns.forever_status_dict.update({col_name: 'TRUE'}) %}
				(c.is_end_of_month = 1)
					{%- else %}
						{%- set monthly_duration = logic_definition['monthly']['duration'] -%}
						{%- set monthly_unit = logic_definition['monthly']['unit'] %}            
				((c.{{ sdts_alias }} BETWEEN DATEADD({{ monthly_unit }}, -{{ monthly_duration }}, GETDATE()) AND GETDATE()) AND (c.is_end_of_month = 1))
					{%- endif -%}
				{% endif -%}

				{%- if 'yearly' in logic_definition.keys() %} OR 
					{%- if logic_definition['yearly']['forever'] is true -%}
						{%- do ns.forever_status_dict.update({col_name: 'TRUE'}) %}
				(c.is_end_of_year = 1)
					{%- else %}
						{%- set yearly_duration = logic_definition['yearly']['duration'] -%}
						{%- set yearly_unit = logic_definition['yearly']['unit'] %}                    
				((c.{{ sdts_alias }} BETWEEN DATEADD({{ yearly_unit }}, -{{ yearly_duration }}, GETDATE()) AND GETDATE()) AND (c.is_end_of_year = 1))
					{%- endif -%}
				{% endif %}
				THEN 1
				ELSE 0
			END AS {{ col_name }}{{ ',' if not loop.last }}
		{% endfor %}
	{%- endif %}

	FROM dynamic c

)

SELECT * FROM log_logic

{%- endmacro -%}
