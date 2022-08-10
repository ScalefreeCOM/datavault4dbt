{#
    This macro creates a link entity, connecting two or more entities, or an entity with itself.
    It can be loaded by one or more source staging tables, if multiple sources share the same buisness definitions.
    Typically a link would only be loaded by multiple sources, if those multiple sources also share the business defintions
    of the hubs, and therefor load the connected hubs together aswell. If multiple sources are used, it is requried that they
    all have the same number of foreign keys inside, otherwise they would not share the same business definition of that link.
#}




{%- macro link(link_hashkey, foreign_hashkeys, source_models, src_ldts=none, src_rsrc=none) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts and src_rsrc are not set. #}

    {%- if src_ldts is none -%}
        {%- set src_ldts = var('dbtvault_scalefree.ldts_alias', 'ldts') -%}
    {%- endif -%}

    {%- if src_rsrc is none -%}
        {%- set src_rsrc = var('dbtvault_scalefree.rsrc_alias', 'rsrc') -%}
    {%- endif -%}

    {{- adapter.dispatch('link', 'dbtvault_scalefree')(link_hashkey=link_hashkey, foreign_hashkeys=foreign_hashkeys,
                                             src_ldts=src_ldts, src_rsrc=src_rsrc,
                                             source_models=source_models) -}}

{%- endmacro -%}


{%- macro default__link(link_hashkey, foreign_hashkeys, source_models, src_ldts, src_rsrc) -%}

{%- if not (foreign_hashkeys is iterable and foreign_hashkeys is not string) -%}

    {%- if execute -%}
        {{ exceptions.raise_compiler_error("Only one foreign key provided for this link. At least two required.") }}
    {%- endif %}

{%- endif -%}

{%- set ns = namespace(last_cte= "", source_included_before = {}) -%}

{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}

{# If no specific link_hk and fk_columns are defined for each source, we apple the values set in the link_hashkey and foreign_hashkeys variable. #}
{%- for source_model in source_models.keys() %}

    {%- if 'fk_columns' not in source_models[source_model].keys() -%}

        {%- do source_models[source_model].update({'fk_columns': foreign_hashkeys}) -%}

    {%- endif -%}

    {%- if 'link_hk' not in source_models[source_model].keys() -%}

        {%- do source_models[source_model].update({'link_hk': link_hashkey}) -%}

    {%- endif -%}

{% endfor %}

{%- set final_columns_to_select = [link_hashkey] + foreign_hashkeys + [src_ldts] + [src_rsrc] -%}

{{ dbtvault_scalefree.prepend_generated_by() }}

WITH

{%- if is_incremental() -%},
distinct_target_hashkeys AS (

    SELECT DISTINCT
    {{ link_hashkey }}
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
    WHERE {{ src_ldts }} != {{ dbtvault_scalefree.string_to_timestamp(timestamp_format, end_of_all_times) }}
    GROUP BY rsrc_static

),
{% endif -%}

{% for source_model in source_models.keys() %}

    {%- set source_number = loop.index | string -%}

    {%- set rsrc_static = source_models[source_model]['rsrc_static'] %}

    src_new_{{ source_number }} AS (

        SELECT
            {{ source_models[source_model]['link_hk'] }} AS {{ link_hashkey }},

            {% for fk in source_models[source_model]['fk_columns']|list -%}
            {{ fk }},
            {%- endfor %}

            {{ src_ldts }},
            {{ src_rsrc }},
            '{{ rsrc_static }}' AS rsrc_static
        FROM {{ ref(source_model|string) }} src

        {%- if is_incremental() and ns.source_included_before[source_model] %}
        INNER JOIN max_ldts_per_rsrc_static_in_target max
            ON max.rsrc_static = '{{ rsrc_static }}'
        WHERE src.{{ src_ldts }} > max.max_ldts
        {%- endif %}

        QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ link_hashkey }} ORDER BY {{ src_ldts }}) = 1

         {%- set ns.last_cte = "src_new_{}".format(source_number) %}

    ),
{%- endfor -%}

{%- if source_models.keys() | length > 1 %}

source_new_union AS (

    {%- for source_model in source_models.keys() -%}

    {%- set source_number = loop.index | string -%}

    SELECT
        {{ link_hashkey }},

        {% for fk in source_models[source_model]['fk_columns']|list %}
            {{ fk }} AS {{ foreign_hashkeys[loop.index - 1] }},
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

    QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ link_hashkey }} ORDER BY {{ src_ldts }}) = 1

    {%- set ns.last_cte = 'earliest_hk_over_all_sources' -%}

),

{%- endif %}

records_to_insert AS (

    SELECT
        {{ dbtvault_scalefree.print_list(final_columns_to_select) }}
    FROM {{ ns.last_cte }}

    {%- if is_incremental() %}
    WHERE {{ link_hashkey }} NOT IN (SELECT * FROM distinct_target_hashkeys)
    {% endif -%}
)

SELECT * FROM records_to_insert

{%- endmacro -%}
