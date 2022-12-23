{%- macro default__ref_table(ref_hub, ref_satellites, src_ldts, src_rsrc, historized, snapshot_trigger_column='is_active', snapshot_relation=none) -%}

{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- set is_current_col_alias = var('datavault4dbt.is_current_col_alias', 'IS_CURRENT') -%}
{%- set ledts_alias = var('datavault4dbt.ledts_alias', 'ledts') -%}

{%- set hub_columns = adapter.get_columns_in_relation(ref(ref_hub)) -%}
{%- set hub_columns_to_exclude = [src_ldts, src_rsrc] -%}
{%- set ref_key_cols = datavault4dbt.process_columns_to_select(hub_columns, hub )%}

{%- set sat_columns_to_exclude = [src_ldts, src_rsrc, ledts_alias] -%}

{%- if not datavault4dbt.is_list(ref_satellites) -%}
    {%- set ref_satellites = [ref_satellites] -%}
{%- endif -%}


WITH 

{%- if historized in ['full', 'latest'] -%}

load_dates AS (

    {%- for satellite in ref_satellites -%}

    SELECT distinct 
        {{ src_ldts }}
    FROM {{ ref(satellite) }}
    {% if not loop.last -%} UNION {%- endif %}

    {%- endfor %}

),

{%- endif %}

ref_table AS (

    SELECT
    {{ datavault4dbt.print_list(datavault4dbt.prefix(columns=ref_key_cols, prefix_str='h')) }},
    ld.{{ date_column }},
    h.{{ src_rsrc }},

    {%- for satellite in ref_satellites %}

    {%- set sat_alias = 's_' + loop.index|string -%}
    {%- set sat_columns = [] -%}
        
        {%- if datavault4dbt.is_list(ref_satellites) %}
            {%- set all_sat_columns = adapter.get_columns_in_relation(ref(satellite)) -%}
            {%- set sat_columns = datavault4dbt.process_columns_to_select(all_sat_columns, sat_columns_to_exclude) -%}
        {%- elif ref_satellites is mapping -%}
            {%- if ref_satellites[satellite] is mapping and 'include' in ref_satellites[satellite].keys() -%}
                {%- set sat_columns = ref_satellites[satellite][include] -%}
            {%- elif ref_satellites[satellite] is mapping and 'exclude' in ref_satellites[satellite].keys() -%}
                {%- set all_sat_columns = adapter.get_columns_in_relation(ref(satellite)) -%}
                {%- set sat_columns = datavault4dbt.process_columns_to_select(all_sat_columns, ref_satellites[satellite]['exclude']) -%}
            {%- elif datavault4dbt.is_list(ref_satellites[satellite]) -%}
                {%- set sat_columns = ref_satellites[satellite] -%}
            {%- else -%}
                {{ exceptions.raise_compiler_error("Invalid definition of ref_satellites. Either a list of satellite names, or a dictionary of satellites, where the key is the satellite name and the value is either a list of columns to select, or another dictionary, with include or exclude as the key, and a list of columns to include/exclude as the value.") }}
            {%- endif -%}
        {%- endif -%}

    {{ datavault4dbt.print_list(datavault4dbt.prefix(columns=sat_columns, prefix_str=sat_alias)) }}
    {%- if not loop.last -%} ,
    {% endif -%}

    {% endfor -%} 

    FROM {{ ref(ref_hub) }} h

    {%- if historized in ['full', 'latest'] -%}
    
        {%- set date_column = src_ldts -%}

    INNER JOIN load_dates ld
        ON {{ datavault4dbt.multikey(columns=src_ldts, prefix=['h', 'ld'], condition='>=') }}

    {% elif snapshot_relation is not none %}

        {%- set date_column = snapshot_trigger_column -%}

    FULL OUTER JOIN {{ ref(snapshot_relation) }} ld
        ON ld.{{ snapshot_trigger_column }} = true
    
    {%- else -%}

        {{ exceptions.raise_compiler_error("If 'historized' is set to 'snapshot', the parameter 'snapshot_relation' must be set. Insert the name of your snapshot v1 view.") }}
    
    {%- endif -%}        

    {% for satellite in ref_satellites %}

        {%- set sat_alias = 's_' + loop.index|string -%}

    LEFT JOIN {{ ref(satellite) }} {{ sat_alias }}
        ON {{ datavault4dbt.multikey(columns=ref_key_cols, prefix=['h', sat_alias], condition='=') }}
        AND  ld.{{ date_column }} BETWEEN {{ sat_alias }}.{{ src_ldts }} AND {{ sat_alias }}.{{ ledts_alias }}
    
    {% endfor %}

    {%- if historized == 'latest' -%}
    WHERE ld.{{ src_ldts }} = (SELECT MAX({{ src_ldts }}) FROM ld)
    {%- endif -%}

) 

SELECT * FROM ref_table

{%- endmacro -%}