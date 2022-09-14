{%- macro default__pit(pit_type, tracked_entity, hashkey, sat_names, ldts, custom_rsrc, ledts, snapshot_relation, snapshot_trigger_column, dimension_key) -%}

{%- set hash = var('dbtvault_scalefree.hash', 'MD5') -%}
{%- set hash_alg, unknown_key, error_key = dbtvault_scalefree.hash_default_values(hash_function=hash) -%}
{%- set rsrc = var('dbtvault_scalefree.rsrc_alias', 'rsrc') -%}

{%- set beginning_of_all_times = var('dbtvault_scalefree.beginning_of_all_times', '0001-01-01T00-00-01') -%}


{{ dbtvault_scalefree.prepend_generated_by() }}

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
        {{ dbtvault_scalefree.as_constant(pit_type) }} as type,
        '{{ custom_rsrc }}' as {{ rsrc }},
        {{ dbtvault_scalefree.hash(columns=[dbtvault_scalefree.as_constant(pit_type), dbtvault_scalefree.prefix([hashkey], 'te'), dbtvault_scalefree.prefix(['sdts'], 'snap')],
                    alias=dimension_key,
                    is_hashdiff=false)   }} ,
        te.{{ hashkey }},
        snap.sdts,
        {% for satellite in sat_names %}
            COALESCE({{ satellite }}.{{ hashkey }}, CAST('{{ zero_key }}' AS STRING)) AS hk_{{ satellite }},
            COALESCE({{ satellite }}.{{ ldts }}, CAST('{{ beginning_of_all_times }}' AS {{ dbtvault_scalefree.type_timestamp() }})) AS {{ ldts }}_{{ satellite }}
            {{- "," if not loop.last }}
        {% endfor %}

    FROM
            {{ ref(tracked_entity) }} te
        FULL OUTER JOIN
            {{ ref(snapshot_relation) }} snap
            ON snap.{{ snapshot_trigger_column }} = true
        {% for satellite in sat_names %}
        {%- set sat_columns = dbtvault_scalefree.source_columns(ref(satellite)) %}
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

    SELECT
        *
    FROM pit_records
    {%- if is_incremental() %}
    WHERE {{ dimension_key }} NOT IN (SELECT * FROM existing_dimension_keys)
    {% endif -%}

)

SELECT * FROM records_to_insert

{%- endmacro -%}
