{%- macro redshift__pit(tracked_entity, hashkey, sat_names, ldts, ledts, sdts, snapshot_relation, refer_to_ghost_records, dimension_key=none, snapshot_trigger_column=none, custom_rsrc=none, pit_type=none, snapshot_optimization=false) -%}

{%- set hash = var('datavault4dbt.hash', 'MD5') -%}
{%- set hash_dtype = var('datavault4dbt.hash_datatype', 'VARCHAR(32)') -%}
{%- set hash_default_values = fromjson(datavault4dbt.hash_default_values(hash_function=hash,hash_datatype=hash_dtype)) -%}
{%- set hash_alg = hash_default_values['hash_alg'] -%}
{%- set unknown_key = hash_default_values['unknown_key'] -%}
{%- set error_key = hash_default_values['error_key'] -%}
{%- set string_default_dtype = datavault4dbt.string_default_dtype() -%}

{%- if hash_dtype == 'BYTES' -%}
    {%- set hashkey_string = 'TO_HEX({})'.format(datavault4dbt.prefix([hashkey],'te')) -%}
{%- else -%}
    {%- set hashkey_string = datavault4dbt.prefix([hashkey],'te') -%}
{%- endif -%}

{%- set rsrc = var('datavault4dbt.rsrc_alias', 'rsrc') -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- if datavault4dbt.is_something(pit_type) -%}
    {%- set hashed_cols = [pit_type, hashkey_string, datavault4dbt.prefix([sdts], 'snap')] -%}
{%- else -%}
    {%- set hashed_cols = [hashkey_string, datavault4dbt.prefix([sdts], 'snap')] -%}
{%- endif -%}

{{ datavault4dbt.prepend_generated_by() }}

SELECT
    {%- if datavault4dbt.is_something(dimension_key) -%}
        {{ datavault4dbt.hash(columns=hashed_cols, alias=dimension_key, is_hashdiff=false) }},
    {%- endif -%}
    te.{{ hashkey }},
    snap.{{ sdts }},
    {% for satellite in sat_names %}
        {%- if refer_to_ghost_records -%}
            COALESCE(
                MAX({{ satellite }}.{{ hashkey }}),
                CAST({{ datavault4dbt.as_constant(column_str=unknown_key) }} as {{ hash_dtype }})
            ) AS hk_{{ satellite }},
            COALESCE(
                MAX({{ satellite }}.{{ ldts }}),
                {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }}
            ) AS {{ ldts }}_{{ satellite }}
        {%- else -%}
            MAX({{ satellite }}.{{ hashkey }}) AS hk_{{ satellite }},
            MAX({{ satellite }}.{{ ldts }}) AS {{ ldts }}_{{ satellite }}
        {%- endif -%}
        {{- "," if not loop.last }}
    {% endfor %}

FROM {{ ref(tracked_entity) }} te

{%- if datavault4dbt.is_something(snapshot_trigger_column) %}
    INNER JOIN {{ ref(snapshot_relation) }} snap
        ON snap.{{ snapshot_trigger_column }} = true
{%- else %}
    CROSS JOIN {{ ref(snapshot_relation) }} snap
{%- endif %}

{% for satellite in sat_names %}
    LEFT JOIN {{ ref(satellite) }} AS {{ satellite }}
        ON {{ satellite }}.{{ hashkey }} = te.{{ hashkey }}
        AND {{ satellite }}.{{ ldts }} <= snap.{{ sdts }}
{% endfor %}

{%- if is_incremental() %}
    WHERE snap.{{ sdts }} NOT IN (
        SELECT DISTINCT {{ sdts }}
        FROM {{ this }}
    )
{%- endif %}

{%- if datavault4dbt.is_something(dimension_key) -%}
GROUP BY 1, 2, 3
{%- else -%}
GROUP BY 1, 2
{%- endif -%}

{%- endmacro -%}
