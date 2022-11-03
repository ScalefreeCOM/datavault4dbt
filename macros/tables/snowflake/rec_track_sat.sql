{%- macro snowflake__rec_track_sat(tracked_hashkey, source_models, src_ldts, src_rsrc, src_stg) -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{# Setting the unknown and error ghost record value for record source column #}
{%- set rsrc_unknown = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') -%}
{%- set rsrc_error = var('datavault4dbt.default_error_rsrc', 'ERROR') -%}

{# Setting the rsrc and stg_alias default datatype and length #}
{%- set rsrc_default_dtype = var('datavault4dbt.rsrc_default_dtype', 'STRING') -%}
{%- set stg_default_dtype = var('datavault4dbt.stg_default_dtype', 'STRING') -%}
{%- set ns = namespace(last_cte = '', source_included_before = {},  source_models_rsrc_dict={},  has_rsrc_static_defined=true) -%}

{%- if source_models is not mapping -%}
    {%- set source_models = {source_models: {}} -%}
{%- endif -%}


{# If no specific hk_column is defined for each source, we apply the values set in the tracked_hashkey input variable. #}
{# If no rsrc_static parameter is defined in a source model then the record source performance look up wont be executed  #}
{%- for source_model in source_models.keys() %}

    {%- if 'hk_column' not in source_models[source_model].keys() -%}
        {%- do source_models[source_model].update({'hk_column': tracked_hashkey}) -%}
    {%- endif -%}

    {%- if 'rsrc_static' not in source_models[source_model].keys() -%}
        {%- set ns.has_rsrc_static_defined = false -%}
    {%- else -%}

        {%- if not (source_models[source_model]['rsrc_static'] is iterable and source_models[source_model]['rsrc_static'] is not string) -%}

            {%- if source_models[source_model]['rsrc_static'] == '' or source_models[source_model]['rsrc_static'] is none -%}
                {%- if execute -%}
                    {{ exceptions.raise_compiler_error("If rsrc_static is defined -> it must not be an empty string ") }}
                {%- endif %}
            {%- else -%}
                {%- do ns.source_models_rsrc_dict.update({source_model : [source_models[source_model]['rsrc_static']] } ) -%}
            {%- endif -%}

        {%- elif source_models[source_model]['rsrc_static'] is iterable -%}
            {%- do ns.source_models_rsrc_dict.update({source_model : source_models[source_model]['rsrc_static'] } ) -%}
        {%- endif -%}

    {%- endif -%}

{% endfor %}

{%- set final_columns_to_select = [tracked_hashkey] + [src_ldts] + [src_rsrc] + [src_stg] -%}

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
    {%- if ns.has_rsrc_static_defined -%}
        rsrc_static_unionized AS (
        {% for source_model in source_models.keys() %}
        {# Create a query with a rsrc_static column with each rsrc_static for each source model. #}
            {%- set source_number = loop.index | string -%}
            {%- set hk_column = source_models[source_model]['hk_column'] -%}
            {%- set rsrc_statics = ns.source_models_rsrc_dict[source_model] -%}

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
            WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
            GROUP BY rsrc_static

        ),
    {%- endif -%}
{% endif -%}

{#
    We deduplicate each source over hashkey + ldts + rsrc_static and if is_incremental only select the rows, where the ldts is later
    than the latest one in the existing satellite for that rsrc_static. If a source is added to the existing satellite, all deduplicated
    rows from that source are loaded into the satellite.
#}

{%- for source_model in source_models.keys() %}

    {%- set source_number = loop.index | string -%}
    {%- set hk_column = source_models[source_model]['hk_column'] -%}
    {%- if ns.has_rsrc_static_defined -%}
        {%- set rsrc_statics = ns.source_models_rsrc_dict[source_model] -%}

        src_new_{{ source_number }} AS (
        {%- for rsrc_static in rsrc_statics %}
            SELECT DISTINCT
                {{ hk_column }} AS {{ tracked_hashkey }},
                {{ src_ldts }},
                CAST('{{ rsrc_static }}' AS {{ rsrc_default_dtype }} ) AS {{ src_rsrc }},
                CAST(UPPER('{{ source_model }}') AS {{ stg_default_dtype }})  AS {{ src_stg }}
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
    {%- else -%}
        src_new_{{ source_number}} AS (
            SELECT DISTINCT
                {{ hk_column }} AS {{ tracked_hashkey }},
                {{ src_ldts }},
                CAST({{ src_rsrc }} AS {{ rsrc_default_dtype }}) AS {{ src_rsrc }},
                CAST(UPPER('{{ source_model }}') AS {{ stg_default_dtype }}) AS {{ src_stg }}
            FROM {{ ref(source_model) }} src
        ),
    {%- endif -%}

    {%- set ns.last_cte = "src_new_{}".format(source_number) %}

{% endfor %}

{#
    If more than one source model is selected, all previously created deduplicated CTEs are unionized.
#}

{%- if source_models.keys() | length > 1 %}

source_new_union AS (
    {% for source_model in source_models.keys() %}
        {%- set hk_column = source_models[source_model]['hk_column'] -%}
        {%- set source_number = loop.index | string -%}

        SELECT
        {{ tracked_hashkey }},
        {{ src_ldts }},
        {{ src_rsrc }},
        {{ src_stg }}
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
