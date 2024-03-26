{%- macro synapse__nh_sat(parent_hashkey, src_payload, src_ldts, src_rsrc, source_model, source_is_single_batch) -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- set source_cols = datavault4dbt.expand_column_list(columns=[parent_hashkey, src_ldts, src_rsrc, src_payload]) -%}
{%- set source_relation = ref(source_model) -%}

{%- set ns = namespace(last_cte='') -%}

{{ datavault4dbt.prepend_generated_by() }}

WITH

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
    WHERE {{ src_ldts }} > (
        SELECT
            COALESCE(MAX({{ src_ldts }}), {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }})
        FROM 
            {{ this }}
        WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
    )
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
