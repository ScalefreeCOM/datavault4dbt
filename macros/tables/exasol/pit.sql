{%- macro exasol__pit(pit_type, tracked_entity, hashkey, sat_names, ldts, custom_rsrc, ledts, sdts, snapshot_relation, snapshot_trigger_column, dimension_key) -%}

{%- set hash = var('datavault4dbt.hash', 'MD5') -%}
{%- set hash_alg, unknown_key, error_key = datavault4dbt.hash_default_values(hash_function=hash) -%}
{%- set rsrc = var('datavault4dbt.rsrc_alias', 'rsrc') -%}
{%- if snapshot_trigger_column is defined and snapshot_trigger_column is not none -%}
    {%- set has_snapshot_trigger_col = true -%}
{%- else -%}
    {%- set has_snapshot_trigger_col = false -%}
{%- endif -%}
{%- set hashkey = hashkey | upper -%}
{%- set dimension_key = dimension_key | upper -%}
{%- set beginning_of_all_times = var('datavault4dbt.beginning_of_all_times', '0001-01-01T00-00-01') -%}
{%- set timestamp_format = var('datavault4dbt.timestamp_format', 'YYYY-mm-ddTHH-MI-SS') -%}
{%- set has_custom_rsrc = false -%}
{%- if not(custom_rsrc is none and custom_rsrc is not string) -%}
    {%- set has_custom_rsrc = true -%}
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
        {{ datavault4dbt.as_constant(pit_type) }} as type,
        {%- if has_custom_rsrc %}
            '{{ custom_rsrc }}' as {{ rsrc }},
        {%- endif %}
        {{ datavault4dbt.hash(columns=[datavault4dbt.as_constant(pit_type), datavault4dbt.prefix([hashkey],'te'), datavault4dbt.prefix([sdts], 'snap')],
                    alias=dimension_key,
                    is_hashdiff=false)   }} ,
        te.{{ hashkey }},
        snap.{{ sdts }},
        {% for satellite in sat_names %}
            COALESCE({{ satellite }}.{{ hashkey }}, CAST('{{ unknown_key }}' AS HASHTYPE)) AS HK_{{ satellite }},
            COALESCE({{ satellite }}.{{ ldts }}, {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }}) AS {{ ldts }}_{{ satellite }}
            {{- "," if not loop.last }}
        {% endfor %}

    FROM
            {{ ref(tracked_entity) }} te
        FULL OUTER JOIN
            {{ ref(snapshot_relation) }} snap
            {% if has_snapshot_trigger_col -%}
                ON snap.{{ snapshot_trigger_column }} = true
            {% else -%}
                ON 1=1
            {%- endif %}
        {% for satellite in sat_names %}
        {%- set sat_columns = datavault4dbt.source_columns(ref(satellite)) %}
        LEFT JOIN {{ ref(satellite) }}
            ON
                {{ satellite }}.{{ hashkey}} = te.{{ hashkey }}
                {%- if ledts|string in sat_columns %}
                    AND snap.{{ sdts }} BETWEEN {{ satellite }}.{{ ldts }} AND {{ satellite }}.{{ ledts }}
                {%- endif -%}
        {% endfor %}
    {%- if has_snapshot_trigger_col -%}
        WHERE snap.{{ snapshot_trigger_column }}
    {%- endif %}

),

records_to_insert AS (

    SELECT DISTINCT *    
    FROM pit_records
    {%- if is_incremental() %}
    WHERE {{ dimension_key }} NOT IN (SELECT * FROM existing_dimension_keys)
    {% endif %}
)

SELECT * FROM records_to_insert

{%- endmacro -%}
