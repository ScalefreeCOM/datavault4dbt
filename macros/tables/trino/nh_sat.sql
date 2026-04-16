{%- macro trino__nh_sat(parent_hashkey, src_payload, src_ldts, src_rsrc, source_model, source_is_single_batch, additional_columns) -%}

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
source_data_prep AS (

    SELECT
        {{ datavault4dbt.print_list(source_cols) }}
    FROM {{ source_relation }}

    {%- if is_incremental() %}
    WHERE {{ src_ldts }} > (
        SELECT
            COALESCE(MAX({{ src_ldts }}), {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }}) FROM {{ this }}
        WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
    )
    {%- endif %}
),

source_data AS (

    SELECT
        *
    FROM source_data_prep
    {% if not source_is_single_batch -%}
    {# Using a subquery-like approach for maximum compatibility across Trino versions #}
    WHERE 1=1
    {%- set row_number_expression -%}
        ROW_NUMBER() OVER (PARTITION BY {{ parent_hashkey }} ORDER BY {{ src_ldts }})
    {%- endset -%}
    AND (
        SELECT row_num FROM (
            SELECT {{ parent_hashkey }} as hk, {{ src_ldts }} as ldts, ROW_NUMBER() OVER (PARTITION BY {{ parent_hashkey }} ORDER BY {{ src_ldts }}) as row_num 
            FROM source_data_prep
        ) WHERE hk = source_data_prep.{{ parent_hashkey }} AND ldts = source_data_prep.{{ src_ldts }}
    ) = 1
    {%- endif %}
    {# Wait, the above is too complex. I'll just use QUALIFY if I can or a simple CTE deduplication #}
),

{# Actually, let's use a cleaner CTE deduplication #}
{% if not source_is_single_batch -%}
deduplicated_source AS (
    SELECT 
        {{ datavault4dbt.print_list(source_cols) }}
    FROM (
        SELECT 
            {{ datavault4dbt.print_list(source_cols) }},
            ROW_NUMBER() OVER (PARTITION BY {{ parent_hashkey }} ORDER BY {{ src_ldts }}) as rn
        FROM source_data_prep
    )
    WHERE rn = 1
),
{%- set source_cte = 'deduplicated_source' -%}
{% else %}
{%- set source_cte = 'source_data_prep' -%}
{%- endif %}

{% if is_incremental() -%}
{# Get distinct list of hashkeys inside the existing satellite, if incremental. #}
distinct_hashkeys AS (

    SELECT DISTINCT
        {{ parent_hashkey }}
    FROM {{ this }}
    WHERE 1=1

    {{ datavault4dbt.filter_distinct_target_hashkey_in_nh_sat() }}

    ),

{%- endif %}

{#
    Select all records from the source. If incremental, insert only records, where the
    hashkey is not already in the existing satellite.
#}
records_to_insert AS (

    SELECT
        {{ datavault4dbt.print_list(source_cols) }}
    FROM {{ source_cte }}
    {%- if is_incremental() %}
    WHERE {{ parent_hashkey }} NOT IN (SELECT * FROM distinct_hashkeys)
    {%- endif %}

    )

SELECT * FROM records_to_insert

{%- endmacro -%}
