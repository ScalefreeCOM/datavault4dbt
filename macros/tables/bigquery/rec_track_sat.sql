{%- macro default__rec_track_sat(tracked_hashkey, source_models, src_ldts, src_rsrc, src_stg, disable_hwm, additional_columns) -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{# Setting the unknown and error ghost record value for record source column #}
{%- set rsrc_unknown = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') -%}
{%- set rsrc_error = var('datavault4dbt.default_error_rsrc', 'ERROR') -%}

{# Setting the rsrc and stg_alias default datatype and length #}
{%- set rsrc_default_dtype = datavault4dbt.string_default_dtype(type='rsrc') -%}
{%- set stg_default_dtype = datavault4dbt.string_default_dtype(type='stg') -%}

{# Setting the ldts to the default datatype for timestamps #}
{% set ldts_default_dtype = datavault4dbt.timestamp_default_dtype() %}

{# Select the additional_columns and put them in an array. If additional_colums is none, then empty array #}
{%- set additional_columns = additional_columns | default([],true) -%}
{%- set additional_columns = [additional_columns] if additional_columns is string else additional_columns -%}

{%- set ns = namespace(last_cte = '', source_included_before = {},  source_models_rsrc_dict={},  has_rsrc_static_defined=true, use_subquery_hwm=false, max_ldts_per_source={}) -%}

{%- if source_models is not mapping and not datavault4dbt.is_list(source_models) -%}
    {%- set source_models = {source_models: {}} -%}
{%- endif -%}

{%- set source_model_values = fromjson(datavault4dbt.source_model_processing(source_models=source_models, parameters={'hk_column':tracked_hashkey})) -%}
{%- set source_models = source_model_values['source_model_list'] -%}
{%- set ns.has_rsrc_static_defined = source_model_values['has_rsrc_static_defined'] -%}
{%- set ns.source_models_rsrc_dict = source_model_values['source_models_rsrc_dict'] -%}
{% if var('datavault4dbt.show_debug_logs', false) %}{{ log('source_models: '~source_models, false) }}{% endif %}

{%- set final_columns_to_select = [tracked_hashkey] + [src_ldts] + [src_rsrc] + [src_stg] + additional_columns -%}

{# Get max(ldts) #}
{% if execute %}
    {%- if is_incremental() and not disable_hwm %}
        {%- if this is none -%}
            {{ exceptions.raise_compiler_error("datavault4dbt: `this` is not available. If this is a unit test on an incremental model, add the model itself as a `given` input so dbt can populate `this`.") }}
        {%- elif this is string -%}
            {%- set ns.use_subquery_hwm = true -%}
        {%- elif source_models | length == 1 and not ns.has_rsrc_static_defined -%}
            {% set max_ldts_query = 'SELECT COALESCE(MAX(' ~ src_ldts ~ '), ' ~ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) ~ ') FROM ' ~ this ~ ' WHERE ' ~ src_ldts ~ ' != ' ~ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) %}
            {% set max_ldts_results = run_query(max_ldts_query) %}
            {% set max_ldts = max_ldts_results.columns[0].values()[0] %}
        {%- endif %}
    {%- endif %}
{% endif %}

{{ datavault4dbt.prepend_generated_by() }}

WITH

{% if is_incremental() %}

    distinct_concated_target AS (
        {%- set concat_columns = [tracked_hashkey, src_ldts, src_rsrc] -%}
        {{ "\n" }}
        SELECT
        {{ datavault4dbt.concat_ws(concat_columns) }} as concat
        FROM {{ this }}
    ),
    {%- if ns.has_rsrc_static_defined and not disable_hwm -%}
        {%- if ns.use_subquery_hwm %}
        rsrc_static_unionized AS (
        {%- endif %}
        {% for source_model in source_models %}
        {# Create a query with a rsrc_static column with each rsrc_static for each source model. #}
              {%- set source_number = source_model.id | string -%}
            {%- set hk_column = source_model['hk_column'] -%}
            {%- set rsrc_statics = ns.source_models_rsrc_dict[source_number] -%}

            {%- set rsrc_static_query_source_count -%}
                SELECT count(*) FROM (
                {%- for rsrc_static in rsrc_statics -%}
                    SELECT 
                    {{ tracked_hashkey }},
                    {{ src_ldts }},
                    '{{ rsrc_static }}' AS rsrc_static
                    FROM {{ this }}
                    WHERE {{ src_rsrc }} like '{{ rsrc_static }}'
                    {%- if not loop.last %} 
                        UNION ALL
                    {% endif -%}
                {%- endfor -%}
                )
            {% endset %}

            {%- set rsrc_static_query_source -%}
                {%- for rsrc_static in rsrc_statics -%}
                    SELECT 
                    {{ tracked_hashkey }},
                    {{ src_ldts }},
                    '{{ rsrc_static }}' AS rsrc_static
                    FROM {{ this }}
                    WHERE {{ src_rsrc }} like '{{ rsrc_static }}'
                    {%- if not loop.last %} 
                        UNION ALL
                    {% endif -%}
                {%- endfor -%}
            {% endset %}

            {%- if ns.use_subquery_hwm %}
            {{ rsrc_static_query_source }}  
            {%- endif %}

            {%- set source_in_target = true -%}
            
            {%- if execute -%}
                {%- set rsrc_static_result = run_query(rsrc_static_query_source_count) -%}

                {%- set row_count = rsrc_static_result.columns[0].values()[0] -%}

                {% if var('datavault4dbt.show_debug_logs', false) %}{{ log('row_count for '~source_model~' is '~row_count, false) }}{% endif %}

                {%- if row_count == 0 -%}
                    {%- set source_in_target = false -%}
                {%- elif not ns.use_subquery_hwm -%}
                    {# Get the max(ldts) per rsrc_static of this source as literal values, to allow partition pruning on the source. #}
                    {%- set max_ldts_query -%}
                        SELECT
                        {%- for rsrc_static in rsrc_statics %}
                            COALESCE(MAX(CASE WHEN {{ src_rsrc }} like '{{ rsrc_static }}' THEN {{ src_ldts }} END), {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }})
                            {%- if not loop.last %},{% endif %}
                        {%- endfor %}
                        FROM {{ this }}
                        WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
                    {% endset %}
                    {%- set max_ldts_results = run_query(max_ldts_query) -%}
                    {%- do ns.max_ldts_per_source.update({source_model.id: {}}) -%}
                    {%- for rsrc_static in rsrc_statics -%}
                        {%- do ns.max_ldts_per_source[source_model.id].update({rsrc_static: max_ldts_results.columns[loop.index0].values()[0]}) -%}
                    {%- endfor -%}
                {%- endif -%}
            {%- endif -%}

            {%- do ns.source_included_before.update({source_model.id: source_in_target}) -%}
            {# Unionize over all sources #}
            {%- if not loop.last and ns.use_subquery_hwm %}
                UNION ALL 
            {% endif -%}

        {% endfor -%}
        {%- if ns.use_subquery_hwm %}
        {%- set ns.last_cte = "rsrc_static_unionized" -%}
        ),

        max_ldts_per_rsrc_static_in_target AS (

            SELECT
                rsrc_static,
                COALESCE(MAX({{ src_ldts }}), {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }}) as max_ldts
            FROM {{ ns.last_cte }}
            WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
            GROUP BY rsrc_static

        ),
        {%- endif %}
    {%- endif %}
{% endif -%}

{#
    We deduplicate each source over hashkey + ldts + rsrc_static and if is_incremental only select the rows, where the ldts is later
    than the latest one in the existing satellite for that rsrc_static. If a source is added to the existing satellite, all deduplicated
    rows from that source are loaded into the satellite.
#}

{%- for source_model in source_models %}

    {%- set source_number = source_model.id | string -%}
    {%- set hk_column = source_model['hk_column'] -%}
    {%- if ns.has_rsrc_static_defined -%}
        {%- set rsrc_statics = ns.source_models_rsrc_dict[source_number|string] -%}

        src_new_{{ source_number }} AS (
        {%- for rsrc_static in rsrc_statics %}
            SELECT DISTINCT
                {{ hk_column }} AS {{ tracked_hashkey }},
                CAST({{ src_ldts }} AS {{ldts_default_dtype  }}) AS {{ src_ldts }},
                CAST('{{ rsrc_static }}' AS {{ rsrc_default_dtype }} ) AS {{ src_rsrc }},
                CAST(UPPER('{{ source_model.name }}') AS {{ stg_default_dtype }})  AS {{ src_stg }}
                {% for col in additional_columns -%}
                ,{{ col }}
                {% endfor -%}
            FROM {{ ref(source_model.name) }} src


            {%- if is_incremental() and ns.has_rsrc_static_defined and ns.source_included_before[source_number|int] and not disable_hwm %}
                {%- if ns.use_subquery_hwm %}
                INNER JOIN max_ldts_per_rsrc_static_in_target max
                    ON max.rsrc_static = '{{ rsrc_static }}'
                WHERE src.{{ src_ldts }} > max.max_ldts
                {%- else %}
                WHERE src.{{ src_ldts }} > '{{ ns.max_ldts_per_source[source_number|int][rsrc_static] }}'
                {%- endif %}
            {%- endif %}
            {%- if not loop.last %}
                UNION ALL
            {% endif -%}
        {% endfor %}

        ),
    {%- else -%}
        src_new_{{ source_number}} AS (
            SELECT DISTINCT
                {{ hk_column }} AS {{ tracked_hashkey }},
                CAST({{ src_ldts }} AS {{ldts_default_dtype  }}) AS {{ src_ldts }},
                CAST({{ src_rsrc }} AS {{ rsrc_default_dtype }}) AS {{ src_rsrc }},
                CAST(UPPER('{{ source_model.name }}') AS {{ stg_default_dtype }}) AS {{ src_stg }}
                {% for col in additional_columns -%}
                ,{{ col }}
                {% endfor -%}
            FROM {{ ref(source_model.name) }} src
            {%- if is_incremental() and source_models | length == 1 and not disable_hwm %}
                {%- if ns.use_subquery_hwm %}
                WHERE src.{{ src_ldts }} > (
            SELECT COALESCE(MAX({{ src_ldts }}), {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }})
            FROM {{ this }}
            WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
            )
                {%- else %}
                WHERE src.{{ src_ldts }} > '{{ max_ldts }}'
                {%- endif %}
            {%- endif %}
        ),
    {%- endif -%}

    {%- set ns.last_cte = "src_new_{}".format(source_number) %}

{% endfor %}

{#
    If more than one source model is selected, all previously created deduplicated CTEs are unionized.
#}

{%- if source_models | length > 1 %}

source_new_union AS (
    {% for source_model in source_models %}
        {%- set hk_column = source_model['hk_column'] -%}
        {%- set source_number = source_model.id | string -%}

        SELECT
        {{ tracked_hashkey }},
        {{ src_ldts }},
        {{ src_rsrc }},
        {{ src_stg }}
        {% for col in additional_columns -%}
        ,{{ col }}
        {% endfor -%}
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
    {{ datavault4dbt.print_list(final_columns_to_select) }}
    FROM {{ ns.last_cte }}
    WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }} 
    AND {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }}
    {%- if is_incremental() %}
        AND {{ datavault4dbt.concat_ws(concat_columns) }} NOT IN (SELECT * FROM distinct_concated_target)
    {% endif %}
)

SELECT * FROM records_to_insert

{%- endmacro -%}
