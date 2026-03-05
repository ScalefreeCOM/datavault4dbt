{%- macro redshift__nh_sat(parent_hashkey, src_payload, src_ldts, src_rsrc, source_model, source_is_single_batch, additional_columns) -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{# Select the additional_columns and put them in an array. If additional_colums is none, then empty array #}
{%- set additional_columns = additional_columns | default([],true) -%}
{%- set additional_columns = [additional_columns] if additional_columns is string else additional_columns -%}

{%- set source_cols = datavault4dbt.expand_column_list(columns=[parent_hashkey, src_ldts, src_rsrc, src_payload, additional_columns]) -%}

{%- set source_relation = ref(source_model) -%}

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
    {%- if not is_incremental() %} redshift_requires_an_alias_if_the_qualify_is_directly_after_the_from {%- endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ parent_hashkey }} ORDER BY {{ src_ldts }}) = 1
        
    {%- endif %} 
),

{% if is_incremental() -%}
{# Get distinct list of hashkeys inside the existing satellite, if incremental. #}
distinct_sat_hashkeys AS (

    SELECT DISTINCT
        sat.{{ parent_hashkey }}
    FROM {{ this }} sat
    WHERE 1=1

    {{ datavault4dbt.filter_distinct_target_hashkey_in_nh_sat(parent_hashkey = parent_hashkey) }}
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
    WHERE {{ parent_hashkey }} NOT IN (SELECT {{ parent_hashkey }} FROM distinct_sat_hashkeys )
    {%- endif %}

    )

SELECT * FROM records_to_insert

{%- endmacro -%}
