{%- macro default__rec_track_sat(tracked_hashkey, source_models, src_ldts, src_rsrc, src_stg) -%}

{%- set end_of_all_times = var('datavault4dbt.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('datavault4dbt.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}

{%- set ns = namespace(last_cte = '', source_included_before = {}) -%}

WITH

{% if is_incremental() -%}

distinct_concated_target AS (

    {%- set concat_columns = [tracked_hashkey, src_ldts, 'rsrc_static'] -%}

    SELECT
        {{ datavault4dbt.concat_ws(concat_columns) }}
    FROM {{ this }}

),

{% for source_model in source_models.keys() %}

    {%- set rsrc_static = source_models[source_model]['rsrc_static'] -%}

    {%- set rsrc_static_query_source -%}
        SELECT *
        FROM {{ this }}
        WHERE rsrc_static like '{{ rsrc_static }}'
    {% endset %}

    {%- set rsrc_static_result = run_query(rsrc_static_query_source) -%}
    {%- set source_in_target = true -%}

    {% if not rsrc_static_result %}
        {%- set source_in_target = false -%}
    {% endif %}

    {%- do ns.source_included_before.update({source_model: source_in_target}) -%}

{% endfor -%}

max_ldts_per_rsrc_static_in_target AS (

    SELECT
        rsrc_static,
        MAX({{ src_ldts }}) as max_ldts
    FROM {{ this }}
    WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
    GROUP BY rsrc_static

),
{% endif -%}

{#
    We deduplicate each source over hashkey + ldts + rsrc_static and if is_incremental only select the rows, where the ldts is later
    than the latest one in the existing satellite for that rsrc_static. If a source is added to the existing satellite, all deduplicated
    rows from that source are loaded into the satellite.
#}

{% for source_model in source_models.keys() %}

    {%- set source_number = loop.index | string -%}

    {%- set rsrc_static = source_models[source_model]['rsrc_static'] -%}

    {%- if 'hk_column' not in source_models[source_model].keys() %}
        {%- set hk_column = tracked_hashkey -%}
    {%- else -%}
        {%- set hk_column = source_models[source_model]['hk_column'] -%}
    {% endif %}

    src_new_{{ source_number }} AS (

        SELECT DISTINCT
            {{ hk_column }} AS {{ tracked_hashkey }},
            {{ src_ldts }},
            '{{ rsrc_static }}' AS rsrc_static
        FROM {{ ref(source_model) }} src


        {%- if is_incremental() and ns.source_included_before[source_model] %}
        INNER JOIN max_ldts_per_rsrc_static_in_target max
            ON max.rsrc_static = '{{ rsrc_static }}'
        WHERE src.{{ src_ldts }} > max.max_ldts
        {%- endif %}

        {%- set ns.last_cte = "src_new_{}".format(source_number) %}

    ),
{%- endfor -%}

{#
    If more than one source model is selected, all previously created deduplicated CTEs are unionized.
#}

{%- if source_models.keys() | length > 1 %}

source_new_union AS (

    {%- for source_model in source_models.keys() -%}

    {%- set source_number = loop.index | string -%}

    SELECT
        {{ tracked_hashkey }},
        {{ src_ldts }},
        rsrc_static
    FROM src_new_{{ source_number }}

    {%- if not loop.last %}
    UNION ALL
    {% endif -%}

    {%- endfor -%}

    {%- set ns.last_cte = 'source_new_union' -%}

),

{%- endif -%}

{#
    Selecting everything, either from the unionized data, or from the single CTE (if single source). Checking against the existing
    satellite to only inserts that are not already inserted, if incremental run.
#}

records_to_insert AS (

    SELECT
        {{ tracked_hashkey }},
        {{ src_ldts }},
        rsrc_static
    FROM {{ ns.last_cte }}

    {%- if is_incremental() %}
    WHERE {{ datavault4dbt.concat_ws(concat_columns) }} NOT IN (SELECT * FROM distinct_concated_target)
    {% endif -%}
)

SELECT * FROM records_to_insert

{%- endmacro -%}
