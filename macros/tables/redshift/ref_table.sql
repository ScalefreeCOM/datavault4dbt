{%- macro redshift__ref_table(ref_hub, ref_satellites, src_ldts, src_rsrc, historized, snapshot_trigger_column='is_active', snapshot_relation=none) -%}

{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- set ref_hub_relation = ref(ref_hub|string) -%}

{%- set is_current_col_alias = var('datavault4dbt.is_current_col_alias', 'IS_CURRENT') -%}
{%- set ledts_alias = var('datavault4dbt.ledts_alias', 'ledts') -%}
{%- set sdts_alias = var('datavault4dbt.sdts_alias', 'sdts') -%}

{%- set include_business_objects_before_appearance = var('datavault4dbt.include_business_objects_before_appearance', 'false') -%}

{{ log('ref_hub_relation: ' ~ ref_hub_relation, false) }}
{%- set hub_columns = datavault4dbt.source_columns(ref_hub_relation) -%}
{{ log('hub_columns: ' ~ hub_columns, false) }}
{%- set hub_columns_to_exclude = [src_ldts, src_rsrc] -%}
{%- set ref_key_cols = datavault4dbt.process_columns_to_select(columns_list=hub_columns, exclude_columns_list=hub_columns_to_exclude )%}
{{ log('ref_key_cols: ' ~ ref_key_cols, false) }}
{%- set sat_columns_to_exclude = [src_ldts, src_rsrc, ledts_alias, is_current_col_alias] + ref_key_cols -%}
{{ log('sat_columns_to_exclude: '~ sat_columns_to_exclude, false) }}

{%- set ref_satellites_dict = {} -%}

{%- if not datavault4dbt.is_list(ref_satellites) and not ref_satellites is mapping -%}
    {%- set ref_satellites = [ref_satellites] -%}
{%- endif -%}

{%- if datavault4dbt.is_list(ref_satellites) -%}
    {%- for ref_satellite in ref_satellites -%}
        {%- do ref_satellites_dict.update({ref_satellite:{}}) -%}
    {%- endfor -%}
{%- else -%}
    {%- set ref_satellites_dict = ref_satellites -%}
{%- endif -%}


WITH 

dates AS (

{% if historized in ['full', 'latest'] -%}

    {%- set date_column = src_ldts -%}


    {{ log('ref_satellites: '~ ref_satellites, false) -}}

    {% if historized == 'full' -%}
    SELECT distinct {{ date_column }} FROM (
    {%- elif historized == 'latest' -%}
    SELECT MAX({{ date_column }}) as {{ date_column }} FROM (
    {%- endif -%}

    {% for satellite in ref_satellites_dict.keys() -%}
    SELECT distinct 
        {{ src_ldts }}
    FROM {{ ref(satellite|string) }}
    WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
    {% if not loop.last -%} UNION {% endif %}
    {%- endfor %}
    ) AS TEST


{% elif snapshot_relation is not none %}

    {%- set date_column = sdts_alias -%}
    
    SELECT 
        {{ date_column }}
    FROM (
        
        SELECT 
            {{ sdts_alias }}
        FROM {{ ref(snapshot_relation) }}
        WHERE {{ snapshot_trigger_column }}
    ) AS TEST 

{#
Caus of whitespace control 
#}

{%- endif %}

{%- if is_incremental() -%}
    WHERE {{ date_column }} > (SELECT MAX({{ date_column }}) FROM {{ this }})
{%- endif -%}


),

ref_table AS (

    SELECT
    {{ datavault4dbt.print_list(list_to_print=ref_key_cols, indent=2, src_alias='h') }},
        ld.{{ date_column }},
        h.{{ src_rsrc }},

    {%- for satellite in ref_satellites_dict.keys() %}

    {%- set sat_alias = 's_' + loop.index|string -%}
    {%- set sat_columns_pre = [] -%}
        
        {%- if ref_satellites_dict[satellite] is mapping and 'include' in ref_satellites_dict[satellite].keys() -%}
            {%- set sat_columns_pre = ref_satellites_dict[satellite]['include'] -%}
        {%- elif ref_satellites_dict[satellite] is mapping and 'exclude' in ref_satellites_dict[satellite].keys() -%}
            {%- set all_sat_columns = datavault4dbt.source_columns(ref(satellite)) -%}
            {%- set sat_columns_pre = datavault4dbt.process_columns_to_select(all_sat_columns, ref_satellites_dict[satellite]['exclude']) -%}
        {%- elif datavault4dbt.is_list(ref_satellites_dict[satellite]) -%}
            {%- set sat_columns_pre = ref_satellites_dict[satellite] -%}
        {%- else -%}
            {%- set all_sat_columns = datavault4dbt.source_columns(ref(satellite)) -%}
            {%- set sat_columns_pre = datavault4dbt.process_columns_to_select(all_sat_columns, sat_columns_to_exclude) -%}
        {%- endif -%}

    {%- set sat_columns = datavault4dbt.process_columns_to_select(sat_columns_pre, sat_columns_to_exclude) -%}
    
    {{- log('sat_columns: '~ sat_columns, false) -}}

    {{ datavault4dbt.print_list(list_to_print=sat_columns, indent=2, src_alias=sat_alias) }}
    {%- if not loop.last -%} ,
    {% endif -%}

    {% endfor %} 

    FROM {{ ref(ref_hub) }} h
    
    FULL OUTER JOIN dates ld
        ON 1 = 1  

    {% for satellite in ref_satellites_dict.keys() %}

        {%- set sat_alias = 's_' + loop.index|string -%}

    LEFT JOIN {{ ref(satellite) }} {{ sat_alias }}
        ON {{ datavault4dbt.multikey(columns=ref_key_cols, prefix=['h', sat_alias], condition='=') }}
        AND  ld.{{ date_column }} BETWEEN {{ sat_alias }}.{{ src_ldts }} AND {{ sat_alias }}.{{ ledts_alias }}
    
    {% endfor %}
    
    {% if include_business_objects_before_appearance == 'false' -%}
    WHERE h.{{ src_ldts }} <= ld.{{ date_column }}
    {% endif %}

) 

SELECT * FROM ref_table

{%- endmacro -%}