{%- macro default__nh_link(link_hashkey, foreign_hashkeys, payload, source_models, src_ldts, src_rsrc) -%}

{%- if not (foreign_hashkeys is iterable and foreign_hashkeys is not string) -%}

    {%- if execute -%}
        {{ exceptions.raise_compiler_error("Only one foreign key provided for this link. At least two required.") }}
    {%- endif %}

{%- endif -%}

{%- set ns = namespace(last_cte= "", source_included_before = {}) -%}

{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}

{# If no specific link_hk, fk_columns, or payload are defined for each source, we apple the values set in the link_hashkey, foreign_hashkeys, and payload variable. #}
{%- for source_model in source_models.keys() %}

    {%- if 'fk_columns' not in source_models[source_model].keys() -%}

        {%- do source_models[source_model].update({'fk_columns': foreign_hashkeys}) -%}

    {%- endif -%}

    {%- if 'link_hk' not in source_models[source_model].keys() -%}

        {%- do source_models[source_model].update({'link_hk': link_hashkey}) -%}

    {%- endif -%}

    {%- if 'payload' not in source_models[source_model].keys() -%}

        {%- do source_models[source_model].update({'payload': payload}) -%}

    {%- endif -%}

{% endfor %}

{%- set final_columns_to_select = [link_hashkey] + foreign_hashkeys + [src_ldts] + [src_rsrc] + payload -%}

{{ dbtvault_scalefree.prepend_generated_by() }}

WITH

{%- if is_incremental() -%},
{# Get all distinct link hashkeys out of the existing link for later incremental logic. #}
distinct_target_hashkeys AS (

    SELECT DISTINCT
    {{ link_hashkey }}
    FROM {{ this }}

),

{% for source_model in source_models.keys() %}
{# Create a new rsrc_static column for each source model. #}

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
    {#  create one unionized table over all source, will be the same as the already existing
        link, but extended by the rsrc_static column. #}

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
{# Use the previously created CTE to calculate the max ldts per rsrc_static. #}

    SELECT
        rsrc_static,
        MAX({{ src_ldts }}) as max_ldts
    FROM {{ ns.last_cte }}
    WHERE {{ src_ldts }} != {{ dbtvault_scalefree.string_to_timestamp(timestamp_format, end_of_all_times) }}
    GROUP BY rsrc_static

),
{% endif -%}

{% for source_model in source_models.keys() %}
{#  Select all deduplicated records from each source, and filter for records that are newer
    than the max ldts inside the existing link, if incremental. #}

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
            '{{ rsrc_static }}' AS rsrc_static,
            {{ dbtvault_scalefree.print_list(source_models[source_model]['payload'])}}
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
{# Unionize the new records from all sources. #}

    {%- for source_model in source_models.keys() -%}

    {%- set source_number = loop.index | string -%}

    SELECT
        {{ link_hashkey }},

        {% for fk in source_models[source_model]['fk_columns']|list %}
            {{ fk }} AS {{ foreign_hashkeys[loop.index - 1] }},
        {% endfor -%}

        {{ src_ldts }},
        {{ src_rsrc }},
        rsrc_static,

        {% for col in source_models[source_model]['payload']|list %}
            {{ col }} AS {{ payload[loop.index - 1] }},
        {% endfor -%}

    FROM src_new_{{ source_number }}

    {%- if not loop.last %}
    UNION ALL
    {% endif -%}

    {%- endfor -%}

    {%- set ns.last_cte = 'source_new_union' -%}

),

earliest_hk_over_all_sources AS (
    {# Deduplicate the unionized records again to only insert the earliest one. #}

    SELECT
        lcte.*
    FROM {{ ns.last_cte }} AS lcte

    QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ link_hashkey }} ORDER BY {{ src_ldts }}) = 1

    {%- set ns.last_cte = 'earliest_hk_over_all_sources' -%}

),

{%- endif %}

records_to_insert AS (
    {# Select everything from the previous CTE, if incremental filter for hashkeys that are not already in the link. #}

    SELECT
        {{ dbtvault_scalefree.print_list(final_columns_to_select) }}
    FROM {{ ns.last_cte }}

    {%- if is_incremental() %}
    WHERE {{ link_hashkey }} NOT IN (SELECT * FROM distinct_target_hashkeys)
    {% endif -%}
)

SELECT * FROM records_to_insert

{%- endmacro -%}
