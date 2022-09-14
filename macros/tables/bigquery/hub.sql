{%- macro default__hub(hashkey, business_keys, src_ldts, src_rsrc, source_models) -%}

{%- set end_of_all_times = var('datavault4dbt.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('datavault4dbt.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}

{%- set rsrc_unknown = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') -%}
{%- set rsrc_error = var('datavault4dbt.default_error_rsrc', 'ERROR') -%}

{%- set ns = namespace(last_cte= "", source_included_before = {}) -%}

{# Select the Business Key column from the first source model definition provided in the hub model and put them in an array. #}

{%- set business_keys = datavault4dbt.expand_column_list(columns=[business_keys]) -%}

{%- for source_model in source_models.keys() %}

    {%- if 'hk_column' not in source_models[source_model].keys() -%}
        {%- do source_models[source_model].update({'hk_column': hashkey}) -%}
    {%- endif -%}

    {%- if 'bk_columns' in source_models[source_model].keys() -%}
        {%- set bk_column_input = source_models[source_model]['bk_columns'] -%}
        {%- set bk_column_input = [bk_column_input] -%}
        {%- do source_models[source_model].update({'bk_columns': bk_column_input}) -%}
    {%- else -%}
        {%- do source_models[source_model].update({'bk_columns': business_keys}) -%}
    {%- endif -%}

    {%- if 'rsrc_static' not in source_models[source_model].keys() -%}
        {%- set unique_rsrc = datavault4dbt.get_distinct_value(source_relation=ref(source_model), column_name=src_rsrc, exclude_values=[rsrc_unknown, rsrc_error]) -%}
        {%- do source_models[source_model].update({'rsrc_static': unique_rsrc}) -%}
    {%- endif -%}

{% endfor %}

{%- if not (source_models is iterable and source_models is not string) -%}
    {{ exceptions.raise_compiler_error("Invalid Source Model definition. Needs to be defined as dictionary for each source model, having the keys 'rsrc_static' and 'bk_column' and optional 'hk_column'.") }}
{%- endif -%}

{%- set final_columns_to_select = [hashkey] + business_keys + [src_ldts] + [src_rsrc] -%}

{{ datavault4dbt.prepend_generated_by() }}

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

    {%- set rsrc_static_query_source -%}
        SELECT {{ this }}.{{ src_rsrc }},
        '{{ rsrc_static }}' AS rsrc_static
        FROM {{ this }}
        WHERE {{ src_rsrc }} like '{{ rsrc_static }}'
    {% endset %}

    rsrc_static_{{ source_number }} AS (

        SELECT
            *,
            '{{ rsrc_static }}' AS rsrc_static
        FROM {{ this }}
        WHERE {{ src_rsrc }} like '{{ rsrc_static }}'

        {%- set ns.last_cte = "rsrc_static_{}".format(source_number) -%}

    ),


    {%- set rsrc_static_result = run_query(rsrc_static_query_source) -%}
    {%- set source_in_target = true -%}

    {% if not rsrc_static_result %}
        {%- set source_in_target = false -%}
    {% endif %}

    {%- do ns.source_included_before.update({source_model: source_in_target}) -%}

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
    WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
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
            {% for bk in source_models[source_model]['bk_columns'] -%}
            {{ bk }},
            {%- endfor %}

            {{ src_ldts }},
            {{ src_rsrc }},
            '{{ rsrc_static }}' AS rsrc_static
        FROM {{ ref(source_model) }} src

        {%- if is_incremental() and ns.source_included_before[source_model] %}
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

        {% for bk in source_models[source_model]['bk_columns'] %}
            {{ bk }} AS {{ business_keys[loop.index - 1] }},
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
        {{ datavault4dbt.print_list(final_columns_to_select) }}
    FROM {{ ns.last_cte }}

    {%- if is_incremental() %}
    WHERE {{ hashkey }} NOT IN (SELECT * FROM distinct_target_hashkeys)
    {% endif -%}
)

SELECT * FROM records_to_insert

{%- endmacro -%}
