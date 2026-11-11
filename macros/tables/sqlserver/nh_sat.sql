{%- macro sqlserver__nh_sat(parent_hashkey, src_payload, src_ldts, src_rsrc, source_model, source_is_single_batch, additional_columns) -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{# Select the additional_columns and put them in an array. If additional_colums is none, then empty array #}
{%- set additional_columns = additional_columns | default([],true) -%}
{%- set additional_columns = [additional_columns] if additional_columns is string else additional_columns -%}

{%- set source_cols = datavault4dbt.expand_column_list(columns=[parent_hashkey, src_ldts, src_rsrc, src_payload, additional_columns]) -%}

{%- set source_relation = ref(source_model) -%}

{%- set ns = namespace(last_cte='', use_subquery_hwm=false) -%}


{%- set source_cols = datavault4dbt.escape_column_names(source_cols) -%}
{%- set parent_hashkey = datavault4dbt.escape_column_names(parent_hashkey) -%}
{%- set src_payload = datavault4dbt.escape_column_names(src_payload) -%}
{%- set src_ldts = datavault4dbt.escape_column_names(src_ldts) -%}
{%- set src_rsrc = datavault4dbt.escape_column_names(src_rsrc) -%}

{# Get max(ldts) as a literal value. Style 121 with 7 fractional digits keeps it a string in dbt, preserving datetime2(7) precision. #}
{% if execute %}
    {%- if is_incremental() %}
        {%- if this is none -%}
            {{ exceptions.raise_compiler_error("datavault4dbt: `this` is not available. If this is a unit test on an incremental model, add the model itself as a `given` input so dbt can populate `this`.") }}
        {%- elif this is string -%}
            {%- set ns.use_subquery_hwm = true -%}
        {%- else -%}
            {% set max_ldts_query = 'SELECT CONVERT(VARCHAR(27), CAST(COALESCE(MAX(' ~ src_ldts ~ '), ' ~ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) ~ ') AS datetime2(7)), 121) AS max_ldts FROM ' ~ this ~ ' WHERE ' ~ src_ldts ~ ' != ' ~ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) %}
            {% set max_ldts_results = run_query(max_ldts_query) %}
            {% set max_ldts = max_ldts_results.columns[0].values()[0] %}
        {%- endif %}
    {%- endif %}
{% endif %}

WITH

{{ datavault4dbt.prepend_generated_by() }}

{# Selecting all source data, that is newer than latest data in sat if incremental #}
source_data AS (

    SELECT
        
        {{ datavault4dbt.print_list(source_cols) }}
        
    {% if not source_is_single_batch -%}
        , ROW_NUMBER() OVER (PARTITION BY {{ parent_hashkey }} ORDER BY {{ src_ldts }}) as RN
    {%- endif %}

    FROM 
        {{ source_relation }}
    {%- if is_incremental() %}
    {%- if ns.use_subquery_hwm %}
    WHERE {{ src_ldts }} > (
        SELECT
            COALESCE(MAX({{ src_ldts }}), {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }})
        FROM 
            {{ this }}
        WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
    )
    {%- else %}
    WHERE {{ src_ldts }} > CONVERT(datetime2(7), '{{ max_ldts }}', 121)
    {%- endif %}
    {%- endif %}

    {%- set ns.last_cte = 'source_data' -%}
),

{% if not source_is_single_batch -%}

deduped_source AS (

    SELECT 
        {{ datavault4dbt.print_list(source_cols) }}
    FROM {{ ns.last_cte }}
    WHERE RN = 1

    {%- set ns.last_cte = 'deduped_source' -%}

),

{%- endif %} 

{% if is_incremental() -%}
{#- Get distinct list of hashkeys inside the existing satellite, if incremental. #}
distinct_hashkeys AS 
(
    SELECT DISTINCT
        {{ parent_hashkey }}
    FROM 
        {{ this }}    
    WHERE 1=1

    {{ datavault4dbt.filter_distinct_target_hashkey_in_nh_sat() }}
    
    ),
{%- endif %}

{#- 
    Select all records from the source. If incremental, insert only records, where the
    hashkey is not already in the existing satellite.
#}
records_to_insert AS (

    SELECT
        {{ datavault4dbt.print_list(source_cols) }}
    FROM {{ ns.last_cte }} cte
    {%- if is_incremental() %}
    WHERE 
        NOT EXISTS (
            SELECT 1
            FROM distinct_hashkeys dth
            WHERE cte.{{ parent_hashkey }} = dth.{{ parent_hashkey }}
        )
    {%- endif %}
    )
SELECT 
  * 
FROM 
  records_to_insert

{%- endmacro -%}
