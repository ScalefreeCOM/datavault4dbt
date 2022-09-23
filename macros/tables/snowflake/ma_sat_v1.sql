{%- macro snowflake__ma_sat_v1(sat_v0, hashkey, hashdiff, ma_attribute, src_ldts, src_rsrc, ledts_alias) -%}

{%- set beginning_of_all_times = var('dbtvault_scalefree.beginning_of_all_times','0001-01-01T00-00-01') -%}
{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times','8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format','%Y-%m-%dT%H-%M-%S') -%}

{%- set source_relation = ref(sat_v0) -%}
{%- set all_columns = dbtvault_scalefree.source_columns(source_relation=source_relation) -%}
{%- set exclude = dbtvault_scalefree.expand_column_list(columns=[hashkey, hashdiff, ma_attribute, src_ldts, src_rsrc]) -%}
{%- set ma_attributes = dbtvault_scalefree.expand_column_list(columns=[ma_attribute]) -%}

{%- set source_columns_to_select = dbtvault_scalefree.process_columns_to_select(all_columns, exclude) -%}

{{ dbtvault_scalefree.prepend_generated_by() }}

WITH 
{#- Getting everything from the underlying v0 satellite. #}
source_satellite AS 
(
    SELECT 
      * 
    FROM 
      {{ source_relation }}
) 
{#- Selecting all distinct loads per hashkey. #}
, distinct_hk_ldts AS 
(
    SELECT DISTINCT 
       {{ hashkey }},
       {{ src_ldts }}
    FROM 
       source_satellite
)
{#- End-dating each ldts for each hashkey, based on earlier ldts per hashkey. #}
, end_dated_loads AS 
(    
    SELECT 
        {{ hashkey }},
        {{ src_ldts }},
        COALESCE(LEAD({{ src_ldts }} - INTERVAL '1 MICROSECOND') OVER (PARTITION BY {{ hashkey }} ORDER BY {{ src_ldts }}),{{ dbtvault_scalefree.string_to_timestamp(timestamp_format['snowflake'],end_of_all_times['snowflake']) }}) AS {{ ledts_alias }}
    FROM 
        distinct_hk_ldts
)
{#- End-date each source record, based on the end-date for each load. #}
, end_dated_source AS 
(
    SELECT 
        src.{{ hashkey }},
        src.{{ src_ldts }},
        src.{{ src_rsrc }},
        edl.{{ ledts_alias }},
        src.{{ hashdiff }},
        {{ dbtvault_scalefree.print_list(ma_attributes) }},
        {{ dbtvault_scalefree.print_list(source_columns_to_select) }}
    FROM source_satellite AS src
    LEFT JOIN end_dated_loads edl
    ON src.{{ hashkey }} = edl.{{ hashkey }}
    AND src.{{ src_ldts }} = edl.{{ src_ldts }}
)
SELECT 
  * 
FROM 
  end_dated_source

{%- endmacro -%}
