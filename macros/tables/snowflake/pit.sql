{%- macro snowflake__pit(pit_type, tracked_entity, hashkey, sat_names, ldts, custom_rsrc, ledts, snapshot_relation, snapshot_trigger_column, dimension_key) -%}

{%- set hash = var('datavault4dbt.hash', 'MD5') -%}
{%- set hash_dtype = var('datavault4dbt.hash_datatype', 'STRING') -%}
{%- set hash_alg, unknown_key, error_key = datavault4dbt.hash_default_values(hash_function=hash,hash_datatype=hash_dtype) -%}
{%- set rsrc = var('datavault4dbt.rsrc_alias', 'rsrc') -%}
{%- set beginning_of_all_times = var('datavault4dbt.beginning_of_all_times', '0001-01-01T00-00-01') -%}

{{ datavault4dbt.prepend_generated_by() }}

WITH 
{%- if is_incremental() %}
existing_dimension_keys AS 
(
    SELECT
       {{ dimension_key }}
    FROM 
       {{ this }}
),
{%- endif %}
pit_records AS 
(    
    SELECT
        {{ datavault4dbt.as_constant(pit_type) }} AS type,
        '{{ custom_rsrc }}' AS {{ rsrc }},
        {{ datavault4dbt.hash(columns=[datavault4dbt.as_constant(pit_type), datavault4dbt.prefix([hashkey], 'te'), datavault4dbt.prefix(['sdts'], 'snap')],
                                   alias=dimension_key,
                                   is_hashdiff=false) }},
        te.{{ hashkey }},
        snap.sdts,
        {%- for satellite in sat_names %}
        COALESCE({{ satellite }}.{{ hashkey }}, CAST({{ unknown_key }} AS {{ hash_dtype }})) AS hk_{{ satellite }},
        COALESCE({{ satellite }}.{{ ldts }}, CAST('{{ beginning_of_all_times['snowflake'] }}' AS {{ datavault4dbt.type_timestamp() }})) AS {{ ldts }}_{{ satellite }}
        {{- "," if not loop.last }}
        {%- endfor %}
    FROM 
        {{ ref(tracked_entity) }} te
    FULL OUTER JOIN 
        {{ ref(snapshot_relation) }} snap
    ON snap.{{ snapshot_trigger_column }} = true
    {%- for satellite in sat_names -%}
    {%- set sat_columns = datavault4dbt.source_columns(ref(satellite)) %}
    LEFT JOIN {{ ref(satellite) }}
    ON {{ satellite }}.{{ hashkey}} = te.{{ hashkey }}
       {%- if ledts|string|upper in sat_columns %}    
       AND snap.sdts BETWEEN {{ satellite }}.{{ ldts }} AND {{ satellite }}.{{ ledts }}
       {%- endif %}
    {%- endfor %}            
    WHERE snap.{{ snapshot_trigger_column }}
)
, records_to_insert AS 
(    
    SELECT
       *
    FROM 
       pit_records
    {%- if is_incremental() %}
    WHERE {{ dimension_key }} NOT IN (SELECT * FROM existing_dimension_keys)
    {% endif %}
)
SELECT 
  * 
FROM 
  records_to_insert

{%- endmacro -%}
