{%- macro snowflake__pit(tracked_entity, hashkey, sat_names, ldts, ledts, sdts, snapshot_relation, dimension_key, refer_to_ghost_records, snapshot_trigger_column=none, custom_rsrc=none, pit_type=none, snapshot_optimization=false) -%}

{%- set hash = datavault4dbt.hash_method() -%}
{%- set hash_dtype = var('datavault4dbt.hash_datatype', 'STRING') -%}
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
  {%- if snapshot_optimization %}
  snapshot_dates as (
    SELECT
      * 
    FROM {{ ref(snapshot_relation) }} 
    {%- if datavault4dbt.is_something(snapshot_trigger_column) %}
      WHERE {{ snapshot_trigger_column }}
    {%- endif %}
  ),

  sdts_max_ldts as ( --get the dts from all relevant snapshots and the max. ldts per satelite from the pit
    SELECT 
      snap.{{ sdts }} 
    {%- for satellite in sat_names %}
      , MAX(pit.{{ ldts }}_{{ satellite }}) max_{{ ldts }}_{{ satellite }}
    {%- endfor %}
    FROM snapshot_dates snap
    LEFT JOIN
    {{ this }} pit
    ON snap.{{ sdts }} = pit.{{ sdts }}
    GROUP BY snap.{{ sdts }}
  ),

  relevant_snapshots as ( --filter to snapshots which have to be handled
    SELECT 
      {{ sdts }}
      {%- for satellite in sat_names %}
        , COALESCE(max_{{ ldts }}_{{ satellite }}, {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }}) as max_{{ ldts }}_{{ satellite }}
      {%- endfor %}
      {% if datavault4dbt.is_something(snapshot_trigger_column) %}
        , true as {{ snapshot_trigger_column }},
      {% endif %}
    FROM sdts_max_ldts 
    WHERE
    {%- for satellite in sat_names %}
      --new snapshot
      max_{{ ldts }}_{{ satellite }} IS NULL OR
      --existing snapshot with max ldts of one sat -> might need to be updated
      max_{{ ldts }}_{{ satellite }} = (SELECT MAX(max_{{ ldts }}_{{ satellite }}) FROM sdts_max_ldts)
      {{ 'OR' if not loop.last }}
    {%- endfor %}
  ),

  {%- else %}
  existing_dimension_keys AS (

    SELECT
      {{ dimension_key }}
    FROM {{ this }}

  ),
  {%- endif %}
  
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
        FULL OUTER JOIN
      {% if snapshot_optimization and is_incremental() %}
      relevant_snapshots snap 
      {%- else %}
            {{ ref(snapshot_relation) }} snap
      {% endif -%}
            {% if datavault4dbt.is_something(snapshot_trigger_column) %}
                ON snap.{{ snapshot_trigger_column }} = true
            {% else %}
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
                COALESCE(LEAD({{ ldts }} - INTERVAL '1 MICROSECOND') OVER (PARTITION BY {{ hashkey }} ORDER BY {{ ldts }}),{{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}) AS {{ ledts }}
            FROM {{ ref(satellite) }}
        ) {{ satellite }}
        {% endif %}
            ON
                {{ satellite }}.{{ hashkey}} = te.{{ hashkey }}
                AND snap.{{ sdts }} BETWEEN {{ satellite }}.{{ ldts }} AND {{ satellite }}.{{ ledts }}
        {% endfor %}
        WHERE 1 = 1
    {% if datavault4dbt.is_something(snapshot_trigger_column) %}
         AND snap.{{ snapshot_trigger_column }}
    {%- endif %}
    {% if snapshot_optimization and is_incremental() %}
            AND (  
        {% for satellite in sat_names %} 
            snap.max_{{ ldts }}_{{ satellite }} <= {{ satellite }}.{{ ldts }}
          {% if not loop.last %}
            OR
          {% endif %}
        {% endfor %}
            )
    {% endif %}

),

records_to_insert AS (

    SELECT DISTINCT *
    FROM pit_records
    {%- if is_incremental() and not snapshot_optimization %}
    WHERE {{ dimension_key }} NOT IN (SELECT * FROM existing_dimension_keys)
    {% endif %}
    ORDER BY {{ sdts }}
)

SELECT * FROM records_to_insert

{%- endmacro -%}
