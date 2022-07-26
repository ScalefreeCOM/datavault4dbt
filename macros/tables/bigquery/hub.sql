{%- macro hub(hashkey, business_key, src_ldts, src_rsrc, source_models) -%}

    {{ return(adapter.dispatch('hub', 'dbtvault_scalefree')(hashkey=hashkey,
                                                  business_key=business_key,
                                                  src_ldts=src_ldts,
                                                  src_rsrc=src_rsrc,
                                                  source_models=source_models)) }}

{%- endmacro -%}                                                  

{%- macro default__hub(hashkey, src_ldts, src_rsrc, source_models) -%}

{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}

{%- set ns = namespace(last_cte= "") -%}

{# Select the Business Key column from the first source model definition provided in the hub model and put them in an array. #}
{%- set bk_columns = [] %}
{%- for source_model in source_models.keys() %}

    {%- if loop.index == 1 -%}

        {%- set bk_column_names = source_models[source_model]['bk_column'] -%}

        {%- if not (bk_column_names is iterable and bk_column_names is not string) -%}

            {%- set bk_column_names = [bk_column_names] -%}
            {%- do source_models[source_model].update('bk_columns': [bk_column_names]) -%}

        {%- endif -%}

        {% set bk_columns = bk_columns + bk_column_names %}

    {%- endif -%}

{% endfor %}

{%- if not (source_models is iterable and source_models is not string) -%}
    {{ exceptions.raise_compiler_error("Invalid Source Model definition. Needs to be defined as dictionary for each source model, having the keys 'name' and 'bk_column' and optional 'hk_column'.") }}
{%- endif -%}

{{ dbtvault_scalefree.prepend_generated_by() }}

WITH


{%- if is_incremental() -%}
distinct_target_hashkeys AS (
    
    SELECT DISTINCT
        {{ hashkey_column }}
    FROM {{ this }}

),

max_ldts_per_source_in_target AS (

    SELECT
        source_model,
        MAX({{ src_ldts }}) as max_ldts
    FROM {{ this }}
    WHERE {{ src_ldts }} != {{ dbtvault_scalefree.string_to_timestamp(timestamp_format, end_of_all_times) }}
    GROUP BY source_model

),
{%- endif -%}

{%- for source_model in source_models.keys() -%}

    {%- set source_number = loop.index | string -%}

    {%- if 'hk_column' not in source_models[source_model].keys() -%}
        {%- set hk_column = hashkey -%}
    {%- else -%}
        {%- set hk_column = source_models[source_model]['hk_column'] -%}
    {%- endif -%}


    src_new_{{ source_number }} AS (

        SELECT 
            {{ hk_column }} AS {{ hashkey_column}},

            {%- for bk in source_models[source_model]['bk_columns'] -%}
            {{ bk }},
            {%- endfor -%}

            {{ src_ldts }},
            {{ src_rsrc }},
            {{ source_model }} AS source_model
        FROM {{ ref(source_model) }} src

        {%- if is_incremental() -%}
        INNER JOIN max_ldts_per_source_in_target max 
            ON max.source_model == '{{ source_model }}'
        WHERE src.{{ src_ldts }} > max.max_ldts
        {%- endif -%}

        QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ hk_column }} ORDER BY {{ src_ldts }}) = 1

         {%- set ns.last_cte = "src_new_{}".format(source_number) %}

    ),
{%- endfor -%}

{%- if source_models.keys() | length > 1 %}

source_new_union AS (

    {%- for source_model in source_models.keys() -%}

    {%- set source_number = loop.index | string -%}

    SELECT
        {{ hashkey_column }},

        {%- for bk in source_models[source_model]['bk_columns'] -%}
            {{ bk }} AS {{ bk_columns[loop.index - 1] }},
        {%- endfor -%}

        {{ src_ldts }},
        {{ src_rsrc }},
        source_model
    FROM src_new_{{ source_number }}

    {%- if not loop.last %}
    UNION ALL
    {%- endif %}

    {%- endfor -%}

    {%- set ns.last_cte = 'source_new_union' -%}

),

earliest_hk_over_all_sources AS (

    SELECT
        lcte.*
    FROM {{ ns.last_cte }} AS lcte

    QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ hashkey_column }} ORDER BY {{ src_ldts }}) = 1

    {%- set ns.last_cte = 'earliest_hk_over_all_sources' -%}

),

{%- endif -%}

records_to_insert AS (

    SELECT 
        lcte.*
    FROM {{ ns.last_cte }} AS lcte

    {%- if is_incremental() %}
    WHERE lcte.{{ hashkey_column }} NOT IN distinct_target_hashkeys
    {% endif -%}
)

SELECT * FROM records_to_insert

{%- endmacro -%}
