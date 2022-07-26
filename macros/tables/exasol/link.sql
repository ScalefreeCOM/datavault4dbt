
{%- macro exasol__link(link_hashkey, foreign_hashkeys, source_model, src_ldts='ldts', src_rsrc='rsrc') -%}

{%- if not (foreign_hashkeys is iterable and foreign_hashkeys is not string) -%}
    
    {%- if execute -%}
        {{ exceptions.raise_compiler_error("Only one foreign key provieded for this link. At least two required.") }}
    {%- endif %}

{%- endif -%}

{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}


{{ dbtvault_scalefree.prepend_generated_by() }}

{% set source_relation = ref(source_model) %}

WITH

source_data AS (

    SELECT
        {{ link_hashkey }},

        {% for foreign_hashkey in foreign_hashkeys -%}
            {{ foreign_hashkey }},
        {% endfor -%}

        {{ src_ldts }},
        {{ src_rsrc }}
    FROM {{ source_relation }}

    {# Reducing the amount of data to only include ldts newer than the existing max ldts in incremental loads #}
    {%- if is_incremental() -%}
    WHERE {{ src_ldts }} > (SELECT MAX({{ src_ldts }}) 
                            FROM {{ this }}
                            WHERE {{ src_ldts }} != {{ dbtvault_scalefree.string_to_timestamp(timestamp_format, end_of_all_times) }} )
    {%- endif -%}

    {# Deudplicate the data and only get the earliest entry for each link hashkey. #}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ link_hashkey }} ORDER BY {{ src_ldts }}) = 1

    {%- set last_cte = 'source_data' %}
)

{%- if is_incremental() -%},
distinct_target_hashkeys AS (

    SELECT DISTINCT 
    {{ link_hashkey }}
    FROM {{ this }}

),

delta AS (

    SELECT
        {{ link_hashkey }},

        {% for foreign_hashkey in foreign_hashkeys -%}
            {{ foreign_hashkey }},
        {% endfor -%}

        {{ src_ldts }},
        {{ src_rsrc }}
    FROM {{ last_cte }}
    WHERE {{ link_hashkey }} NOT IN distinct_target_hashkeys

    {%- set last_cte = 'delta' -%}
)
{%- endif %}

SELECT * FROM {{ last_cte }}

{%- endmacro -%}


