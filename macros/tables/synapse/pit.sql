{%- macro synapse__pit(tracked_entity, hashkey, sat_names, ldts, ledts, sdts, snapshot_relation, dimension_key, refer_to_ghost_records, snapshot_trigger_column=none, custom_rsrc=none, pit_type=none) -%}

{%- set hash = var('datavault4dbt.hash', 'MD5') -%}
{%- set hash_dtype = var('datavault4dbt.hash_datatype', 'BINARY(16)') -%}
{%- set hash_default_values = fromjson(datavault4dbt.hash_default_values(hash_function=hash,hash_datatype=hash_dtype)) -%}
{%- set hash_alg = hash_default_values['hash_alg'] -%}
{%- set unknown_key = hash_default_values['unknown_key'] -%}
{%- set error_key = hash_default_values['error_key'] -%}

{%- set rsrc = var('datavault4dbt.rsrc_alias', 'rsrc') -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- if datavault4dbt.is_something(pit_type) -%}
    {%- set quote = "'" -%}
    {%- set pit_type_quoted = quote + pit_type + quote -%}
    {%- set hashed_cols = [pit_type_quoted, datavault4dbt.prefix([hashkey],'te'), datavault4dbt.prefix([sdts], 'snap')] -%}
{%- else -%}
    {%- set hashed_cols = [datavault4dbt.prefix([hashkey],'te'), datavault4dbt.prefix([sdts], 'snap')] -%}
{%- endif -%}

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
        {{ datavault4dbt.hash(columns=hashed_cols,
                    alias=dimension_key,
                    is_hashdiff=false)   }} ,
        te.{{ hashkey }},
        snap.{{ sdts }},
        {%- for satellite in sat_names %}
          {% if refer_to_ghost_records %}
            COALESCE({{ satellite }}.{{ hashkey }}, CAST({{ datavault4dbt.as_constant(column_str=unknown_key) }} as {{ hash_dtype }})) AS hk_{{ satellite }},
            COALESCE({{ satellite }}.{{ ldts }}, {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }}) AS {{ ldts }}_{{ satellite }}
          {% else %}
            {{ satellite }}.{{ hashkey }} AS hk_{{ satellite }},
            {{ satellite }}.{{ ldts }} AS {{ ldts }}_{{ satellite }}
          {% endif %}
        {{- "," if not loop.last }}
        {%- endfor %}

    FROM
            {{ ref(tracked_entity) }} te
        INNER JOIN
            {{ ref(snapshot_relation) }} snap
            {% if datavault4dbt.is_something(snapshot_trigger_column) -%}
                ON snap.{{ snapshot_trigger_column }} = 1
            {% else -%}
                ON 1=1
            {%- endif %}
        {% for satellite in sat_names %}
        {%- set sat_columns = datavault4dbt.source_columns(ref(satellite)) %}
        {%- if ledts|string|lower in sat_columns|map('lower') %}
        LEFT JOIN {{ ref(satellite) }}
        {%- else %}
        LEFT JOIN (
            SELECT
                {{ hashkey }},
                {{ ldts }},
                COALESCE(LEAD(DATEADD(ns, -100, {{ ldts }})) OVER (PARTITION BY {{ hashkey }} ORDER BY {{ ldts }}),{{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}) AS {{ ledts }}
            FROM {{ ref(satellite) }}
        ) {{ satellite }}
        {% endif %}
            ON
                {{ satellite }}.{{ hashkey}} = te.{{ hashkey }}
                AND snap.{{ sdts }} BETWEEN {{ satellite }}.{{ ldts }} AND {{ satellite }}.{{ ledts }}
        {% endfor %}
),

records_to_insert AS (

    SELECT DISTINCT *
    FROM pit_records pr
    {%- if is_incremental() %} 
        WHERE 
        NOT EXISTS (
            SELECT 1
            FROM existing_dimension_keys edk
            WHERE 1=1
                AND edk.{{ dimension_key }} = pr.{{ dimension_key }}
        )
    {% endif -%}

)

SELECT * FROM records_to_insert

{%- endmacro -%}
