{%- macro default__pit(pit_type, tracked_entity, hashkey, sat_names, ldts, custom_rsrc, ledts, sdts, snapshot_relation, snapshot_trigger_column, dimension_key) -%}

{%- set hash = var('datavault4dbt.hash', 'MD5') -%}
{%- set hash_dtype = var('datavault4dbt.hash_datatype', 'STRING') -%}
{%- set hash_default_values = fromjson(datavault4dbt.hash_default_values(hash_function=hash,hash_datatype=hash_dtype)) -%}
{%- set hash_alg = hash_default_values['hash_alg'] -%}
{%- set unknown_key = hash_default_values['unknown_key'] -%}
{%- set error_key = hash_default_values['error_key'] -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}

{{ datavault4dbt.prepend_generated_by() }}

WITH

{%- if is_incremental() %}

existing_dimension_keys AS (

    SELECT
        {{ dimension_key }}
    FROM {{ this }}

),

{%- endif %}

pit_records AS (

    SELECT
        
        {% if datavault4dbt.is_something(pit_type) -%}
            {{ datavault4dbt.as_constant(pit_type) }} as type,
        {%- endif %}
        {% if datavault4dbt.is_something(custom_rsrc) -%}
        '{{ custom_rsrc }}' as {{ rsrc }},
        {%- endif %}
        {{ datavault4dbt.hash(columns=[datavault4dbt.as_constant(pit_type), datavault4dbt.prefix([hashkey], 'te'), datavault4dbt.prefix(['sdts'], 'snap')],
                    alias=dimension_key,
                    is_hashdiff=false)   }} ,
        te.{{ hashkey }},
        snap.sdts,
        {% for satellite in sat_names %}
            COALESCE({{ satellite }}.{{ hashkey }}, CAST('{{ unknown_key }}' AS {{ hash_dtype }})) AS hk_{{ satellite }},
            COALESCE({{ satellite }}.{{ ldts }}, CAST('{{ beginning_of_all_times }}' AS {{ datavault4dbt.type_timestamp() }})) AS {{ ldts }}_{{ satellite }}
            {{- "," if not loop.last }}
        {% endfor %}

    FROM
            {{ ref(tracked_entity) }} te
        FULL OUTER JOIN
            {{ ref(snapshot_relation) }} snap
            ON snap.{{ snapshot_trigger_column }} = true
        {% for satellite in sat_names %}
        {%- set sat_columns = datavault4dbt.source_columns(ref(satellite)) %}
        LEFT JOIN {{ ref(satellite) }}
            ON
                {{ satellite }}.{{ hashkey}} = te.{{ hashkey }}
                {%- if ledts|string in sat_columns %}
                    AND snap.sdts BETWEEN {{ satellite }}.{{ ldts }} AND {{ satellite }}.{{ ledts }}
                {%- endif -%}
        {% endfor %}
    WHERE snap.{{ snapshot_trigger_column }}

),

records_to_insert AS (

    SELECT *
    FROM pit_records
    {%- if is_incremental() %}
    WHERE {{ dimension_key }} NOT IN (SELECT * FROM existing_dimension_keys)
    {% endif -%}

)

SELECT * FROM records_to_insert

{%- endmacro -%}
