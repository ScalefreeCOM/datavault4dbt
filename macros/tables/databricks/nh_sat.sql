{%- macro databricks__nh_sat(parent_hashkey, src_payload, src_ldts, src_rsrc, source_model, source_is_single_batch) -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- set source_cols = datavault4dbt.expand_column_list(columns=[parent_hashkey, src_ldts, src_rsrc, src_payload]) -%}

{%- set source_relation = ref(source_model) -%}

{%- set source_cols = datavault4dbt.escape_column_names(source_cols) -%}
{%- set parent_hashkey = datavault4dbt.escape_column_names(parent_hashkey) -%}
{%- set src_payload = datavault4dbt.escape_column_names(src_payload) -%}
{%- set src_ldts = datavault4dbt.escape_column_names(src_ldts) -%}
{%- set src_rsrc = datavault4dbt.escape_column_names(src_rsrc) -%}

{{ datavault4dbt.prepend_generated_by() }}

WITH

{# Selecting all source data, that is newer than latest data in sat if incremental #}
source_data AS (

    SELECT
        {{ datavault4dbt.print_list(source_cols) }}
    FROM {{ source_relation }}

    {%- if is_incremental() %}
    WHERE {{ src_ldts }} > (
        SELECT
            MAX({{ src_ldts }}) FROM {{ this }}
        WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
    )
    {%- endif %}

    {% if not source_is_single_batch -%}

    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY {{ parent_hashkey }} ORDER BY {{ src_ldts }}) = 1
        
    {%- endif %}        

),

{% if is_incremental() -%}
{# Get distinct list of hashkeys inside the existing satellite, if incremental. #}
distinct_hashkeys AS (

    SELECT DISTINCT
        {{ parent_hashkey }}
    FROM {{ this }}

    ),

{%- endif %}

{#
    Select all records from the source. If incremental, insert only records, where the
    hashkey is not already in the existing satellite.
#}
records_to_insert AS (

    SELECT
        {{ datavault4dbt.print_list(source_cols) }}
    FROM source_data
    {%- if is_incremental() %}
    WHERE {{ parent_hashkey }} NOT IN (SELECT * FROM distinct_hashkeys)
    {%- endif %}

    )

SELECT * FROM records_to_insert

{%- endmacro -%}
