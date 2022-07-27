{%- macro hub(hashkey, src_ldts, src_rsrc, source_models) -%}

    {{ return(adapter.dispatch('hub', 'dbtvault_scalefree')(hashkey=hashkey,
                                                  src_ldts=src_ldts,
                                                  src_rsrc=src_rsrc,
                                                  source_models=source_models)) }}

{%- endmacro -%}                                                  

{%- macro default__hub(hashkey, src_ldts, src_rsrc, source_models) -%}

{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}

{%- set ns = namespace(last_cte= "", bk_columns = []) -%}

{# Select the Business Key column from the first source model definition provided in the hub model and put them in an array. #}

{%- for source_model in source_models.keys() %}    

    {%- set bk_column_input = source_models[source_model]['bk_column'] -%}

    {%- if not (bk_column_input is iterable and bk_column_input is not string) -%}

        {%- set bk_column_input = [bk_column_input] -%}
        {%- do source_models[source_model].update({'bk_columns': bk_column_input}) -%}

    {%- endif -%}

    {%- if loop.index == 1 -%}

        {% set ns.bk_columns = ns.bk_columns + bk_column_input %}

    {%- endif -%}

{% endfor %}

{%- if not (source_models is iterable and source_models is not string) -%}
    {{ exceptions.raise_compiler_error("Invalid Source Model definition. Needs to be defined as dictionary for each source model, having the keys 'rsrc_static' and 'bk_column' and optional 'hk_column'.") }}
{%- endif -%}

{%- set final_columns_to_select = [hashkey] + ns.bk_columns + [src_ldts] + [src_rsrc] -%}

{{ dbtvault_scalefree.prepend_generated_by() }}

WITH


{% if is_incremental() -%}
distinct_target_hashkeys AS (
    
    SELECT DISTINCT
        {{ hashkey }}
    FROM {{ this }}

),

{% for source_model in source_models.keys() %}

    {%- set source_number = loop.index | string -%}
    {%- set rsrc_static = source_models[source_model]['rsrc_static'] -%}
    
    rsrc_static_{{ source_number }} AS (
        
        SELECT 
            *,
            '{{ rsrc_static }}' AS rsrc_static
        FROM {{ this }}
        WHERE {{ src_rsrc }} like '{{ rsrc_static }}'

        {%- set ns.last_cte = "rsrc_static_{}".format(source_number) -%}

    ),
{% endfor -%}

{%- if source_models.keys() | length > 1 %}

rsrc_static_union AS (

    {% for source_model in source_models.keys() %}
    {%- set source_number = loop.index | string -%}

    SELECT * FROM rsrc_static_{{ source_number }}

    {%- if not loop.last %}
    UNION ALL
    {% endif -%}
    {%- endfor %}
    {%- set ns.last_cte = "rsrc_static_union".format(source_number) -%}
),

{%- endif %}

max_ldts_per_rsrc_static_in_target AS (

    SELECT
        rsrc_static,
        MAX({{ src_ldts }}) as max_ldts
    FROM {{ ns.last_cte }}
    WHERE {{ src_ldts }} != {{ dbtvault_scalefree.string_to_timestamp(timestamp_format, end_of_all_times) }}
    GROUP BY rsrc_static

), 
{% endif -%}

{% for source_model in source_models.keys() %}

    {%- set source_number = loop.index | string -%}

    {%- set rsrc_static = source_models[source_model]['rsrc_static'] -%}

    {%- if 'hk_column' not in source_models[source_model].keys() %}
        {%- set hk_column = hashkey -%}
    {%- else -%}
        {%- set hk_column = source_models[source_model]['hk_column'] -%}
    {% endif %}

    src_new_{{ source_number }} AS (

        SELECT 
            {{ hk_column }} AS {{ hashkey }},

            {% for bk in source_models[source_model]['bk_columns']|list -%}
            {{ bk }},
            {%- endfor %}

            {{ src_ldts }},
            {{ src_rsrc }},
            '{{ rsrc_static }}' AS rsrc_static
        FROM {{ ref(source_model) }} src

        {%- if is_incremental() %}
        INNER JOIN max_ldts_per_rsrc_static_in_target max 
            ON max.rsrc_static = '{{ rsrc_static }}'
        WHERE src.{{ src_ldts }} > max.max_ldts
        {%- endif %}

        QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ hk_column }} ORDER BY {{ src_ldts }}) = 1

         {%- set ns.last_cte = "src_new_{}".format(source_number) %}

    ),
{%- endfor -%}

{%- if source_models.keys() | length > 1 %}

source_new_union AS (

    {%- for source_model in source_models.keys() -%}

    {%- set source_number = loop.index | string -%}

    SELECT
        {{ hashkey }},

        {% for bk in source_models[source_model]['bk_columns']|list %}
            {{ bk }} AS {{ ns.bk_columns[loop.index - 1] }},
        {% endfor -%}

        {{ src_ldts }},
        {{ src_rsrc }},
        rsrc_static
    FROM src_new_{{ source_number }}

    {%- if not loop.last %}
    UNION ALL
    {% endif -%}

    {%- endfor -%}

    {%- set ns.last_cte = 'source_new_union' -%}

),

earliest_hk_over_all_sources AS (

    SELECT
        lcte.*
    FROM {{ ns.last_cte }} AS lcte

    QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ hashkey }} ORDER BY {{ src_ldts }}) = 1

    {%- set ns.last_cte = 'earliest_hk_over_all_sources' -%}

),

{%- endif %}

records_to_insert AS (

    SELECT 
        {{ dbtvault_scalefree.print_list(final_columns_to_select) }}
    FROM {{ ns.last_cte }}

    {%- if is_incremental() %}
    WHERE {{ hashkey }} NOT IN (SELECT * FROM distinct_target_hashkeys)
    {% endif -%}
)

SELECT * FROM records_to_insert

{%- endmacro -%}
