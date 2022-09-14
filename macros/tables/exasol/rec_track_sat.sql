{%- macro exasol__rec_track_sat(tracked_hashkey, source_models, src_ldts, src_rsrc) -%}

{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set beginning_of_all_times = var('dbtvault_scalefree.beginning_of_all_times', '0001-01-01T00-00-01') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', 'YYYY-mm-ddTHH-MI-SS') -%}

{%- set ns = namespace(last_cte = '', source_included_before = {},  source_models_rsrc_dict={}, all_rsrc=[]) -%}

{%- if source_models is not mapping -%}
    {%- if execute -%}
        {{ exceptions.raise_compiler_error("Invalid Source Model definition. Needs to be defined as dictionary for each source model.") }}
    {%- endif %}
{%- endif -%}


{# If no specific hk_column is defined for each source, we apply the values set in the tracked_hashkey input variable. #}
{# If no rsrc_static parameter is defined in ANY of the source models then the whole code block of record_source performance lookup is not executed  #}
{# For the use of record_source performance lookup it is required that every source model has the parameter rsrc_static defined and it cannot be an empty string #}
{%- for source_model in source_models.keys() %}

    {%- if 'hk_column' in source_models[source_model].keys() -%}
        {%- set hk_column_input = source_models[source_model]['hk_column'] -%}

        {%- do source_models[source_model].update({'hk_column': hk_column_input}) -%}
    {%- else -%}
        {%- do source_models[source_model].update({'hk_column': tracked_hashkey}) -%}
    {%- endif -%}

    {%- if 'rsrc_static' not in source_models[source_model].keys() -%}
        {%- if execute -%}
            {{ exceptions.raise_compiler_error("rsrc_static must be defined for each source model ") }}
        {%- endif %}
    {%- else -%}

        {%- if not (source_models[source_model]['rsrc_static'] is iterable and source_models[source_model]['rsrc_static'] is not string) -%}

            {%- if source_models[source_model]['rsrc_static'] == '' or source_models[source_model]['rsrc_static'] is none -%}
                {%- if execute -%}
                    {{ exceptions.raise_compiler_error("rsrc_static must not be an empty string ") }}
                {%- endif %}
            {%- else -%}
                {%- do ns.source_models_rsrc_dict.update({source_model : [source_models[source_model]['rsrc_static']] } ) -%}
                {%- do ns.all_rsrc.append(source_models[source_model]['rsrc_static']) -%}
            {%- endif -%}

        {%- elif source_models[source_model]['rsrc_static'] is iterable -%}
            {%- do ns.source_models_rsrc_dict.update({source_model : source_models[source_model]['rsrc_static'] } ) -%}
            {%- for rsrc in source_models[source_model]['rsrc_static'] -%}
                {%- do ns.all_rsrc.append(rsrc) -%}
            {%- endfor -%}
        {%- endif -%}

    {%- endif -%}

{% endfor %}

{%- set final_columns_to_select = [tracked_hashkey] + [src_ldts] + [src_rsrc] -%}

{{ dbtvault_scalefree.prepend_generated_by() }}

WITH

{% if is_incremental() %}

    distinct_concated_target AS (
        {%- set concat_columns = [tracked_hashkey, src_ldts, src_rsrc] -%}
        {{ "\n" }}
        SELECT
        {{ dbtvault_scalefree.concat_ws(concat_columns) }} as concat
        FROM {{ this }}
        WHERE {{ src_ldts }} != {{ dbtvault_scalefree.string_to_timestamp(timestamp_format, end_of_all_times) }} and
        {{ src_ldts }} != {{ dbtvault_scalefree.string_to_timestamp(timestamp_format, beginning_of_all_times) }}

    ),

    rsrc_static_unionized AS (
    {% for source_model in source_models.keys() %}
    {# Create a query with a rsrc_static column with each rsrc_static for each source model. #}
        {%- set source_number = loop.index | string -%}
        {%- set hk_column = source_models[source_model]['hk_column'] -%}
        {%- set rsrc_statics = ns.source_models_rsrc_dict[source_model] -%}

        {%- set rsrc_static_query_source -%}
            {%- for rsrc_static in rsrc_statics -%}
                SELECT 
                {{ hk_column }} as {{ tracked_hashkey }},
                {{ src_ldts }},
                '{{ rsrc_static }}' AS rsrc_static
                FROM {{ this }}
                WHERE {{ src_rsrc }} like '{{ rsrc_static }}'
                {%- if not loop.last %} 
                    UNION ALL
                {% endif -%}
            {%- endfor -%}
        {% endset %}

        {{ rsrc_static_query_source }}  

        {%- set rsrc_static_result = run_query(rsrc_static_query_source) -%}
        {%- set source_in_target = true -%}

        {% if not rsrc_static_result %}
            {%- set source_in_target = false -%}
        {% endif %}

        {%- do ns.source_included_before.update({source_model: source_in_target}) -%}
        {# Unionize over all sources #}
        {%- if not loop.last %}
            UNION ALL 
        {% endif -%}

    {% endfor -%}
    {%- set ns.last_cte = "rsrc_static_unionized" -%}
    ),

    max_ldts_per_rsrc_static_in_target AS (

        SELECT
            rsrc_static,
            MAX({{ src_ldts }}) as max_ldts
        FROM {{ ns.last_cte }}
        WHERE {{ src_ldts }} != {{ dbtvault_scalefree.string_to_timestamp(timestamp_format, end_of_all_times) }}
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

    {%- set rsrc_statics = ns.source_models_rsrc_dict[source_model] -%}

    {%- set hk_column = source_models[source_model]['hk_column'] -%}

    src_new_{{ source_number }} AS (
    {%- for rsrc_static in rsrc_statics %}
        SELECT DISTINCT
            {{ hk_column }} AS {{ tracked_hashkey }},
            {{ src_ldts }},
            '{{ rsrc_static }}' AS {{ src_rsrc }}
        FROM {{ ref(source_model) }} src


        {%- if is_incremental() and ns.source_included_before[source_model] %}
            INNER JOIN max_ldts_per_rsrc_static_in_target max
                ON max.rsrc_static = '{{ rsrc_static }}'
            WHERE src.{{ src_ldts }} > max.max_ldts
        {%- endif %}

        {%- if not loop.last %}
            UNION ALL
        {% endif -%}
    {% endfor %}

    ),

    {%- set ns.last_cte = "src_new_{}".format(source_number) %}

{%- endfor -%}

{#
    If more than one source model is selected, all previously created deduplicated CTEs are unionized.
#}

{%- if source_models.keys() | length > 1 %}

source_new_union AS (
    {% for source_model in source_models.keys() %}
        {%- set hk_column = source_models[source_model]['hk_column'] -%}
        {%- set source_number = loop.index | string -%}

        SELECT
        {{ hk_column }} as {{ tracked_hashkey }},
        {{ src_ldts }},
        {{ src_rsrc }}
        FROM src_new_{{ source_number }}

        {%- if not loop.last %}
        UNION ALL
        {% endif -%}

    {% endfor %}

    {%- set ns.last_cte = 'source_new_union' -%}

),


{%- endif -%}


{#
    Selecting everything, either from the unionized data, or from the single CTE (if single source). Checking against the existing
    satellite to only inserts that are not already inserted, if incremental run.
#}

records_to_insert AS (

    SELECT
    {{ dbtvault_scalefree.print_list(final_columns_to_select) }}
    FROM {{ ns.last_cte }}
    WHERE {{ src_ldts }} != {{ dbtvault_scalefree.string_to_timestamp(timestamp_format, end_of_all_times) }} 
    AND {{ src_ldts }} != {{ dbtvault_scalefree.string_to_timestamp(timestamp_format, beginning_of_all_times) }}
    {%- if is_incremental() %}
        AND {{ dbtvault_scalefree.concat_ws(concat_columns) }} NOT IN (SELECT * FROM distinct_concated_target)
    {% endif %}
)

SELECT * FROM records_to_insert

{%- endmacro -%}
