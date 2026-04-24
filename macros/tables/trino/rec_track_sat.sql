{%- macro trino__rec_track_sat(tracked_hashkey, source_models, src_ldts, src_rsrc, src_stg, disable_hwm, additional_columns) -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{# Setting the unknown and error ghost record value for record source column #}
{%- set rsrc_unknown = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') -%}
{%- set rsrc_error = var('datavault4dbt.default_error_rsrc', 'ERROR') -%}

{# Setting the rsrc and stg_alias default datatype and length #}
{%- set rsrc_default_dtype = datavault4dbt.string_default_dtype(type='rsrc') -%}
{%- set stg_default_dtype = datavault4dbt.string_default_dtype(type='stg') -%}

{# Select the additional_columns and put them in an array. If additional_colums is none, then empty array #}
{%- set additional_columns = additional_columns | default([],true) -%}
{%- set additional_columns = [additional_columns] if additional_columns is string else additional_columns -%}

{%- set ns = namespace(last_cte = '', source_included_before = {},  source_models_rsrc_dict={},  has_rsrc_static_defined=true) -%}

{%- if source_models is not mapping and not datavault4dbt.is_list(source_models) -%}
    {%- set source_models = {source_models: {}} -%}
{%- endif -%}

{%- set source_model_values = fromjson(datavault4dbt.source_model_processing(source_models=source_models, parameters={'hk_column':tracked_hashkey})) -%}
{%- set source_models = source_model_values['source_model_list'] -%}
{%- set ns.has_rsrc_static_defined = source_model_values['has_rsrc_static_defined'] -%}
{%- set ns.source_models_rsrc_dict = source_model_values['source_models_rsrc_dict'] -%}
{{ log('source_models: '~source_models, false) }}

{%- set final_columns_to_select = [tracked_hashkey] + [src_ldts] + [src_rsrc] + [src_stg] + additional_columns -%}

{{ datavault4dbt.prepend_generated_by() }}

WITH

{% if is_incremental() %}

    distinct_concated_target AS (
        {%- set concat_columns = [tracked_hashkey, src_ldts, src_rsrc] -%}
        SELECT
        {{ datavault4dbt.concat_ws(concat_columns) }} as concat
        FROM {{ this }}
    ),
    {%- if ns.has_rsrc_static_defined and not disable_hwm -%}
        rsrc_static_unionized AS (
        {% for source_model in source_models %}
        {# Create a query with a rsrc_static column with each rsrc_static for each source model. #}
              {%- set source_number = source_model.id | string -%}
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
                ) AS test
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

            {{ rsrc_static_query_source }}  

            {%- if not loop.last %} UNION ALL {% endif -%}

            {%- set source_in_target = true -%}
            
            {%- if execute -%}
                {%- set rsrc_static_result = run_query(rsrc_static_query_source_count) -%}

                {%- set row_count = rsrc_static_result.columns[0].values()[0] -%}

                {{ log('row_count for '~source_model~' is '~row_count, false) }}

                {%- if row_count == 0 -%}
                    {%- set source_in_target = false -%}
                {%- endif -%}
            {%- endif -%}

            {%- do ns.source_included_before.update({source_model.id: source_in_target}) -%}
        {% endfor -%}
        ),

        max_ldts_per_rsrc_static_in_target AS (

            SELECT
                rsrc_static,
                MAX({{ src_ldts }}) as max_ldts
            FROM rsrc_static_unionized
            WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
            GROUP BY rsrc_static

        ),
    {%- endif %}
{% endif -%}

{% for source_model in source_models %}

    {%- set source_number = source_model.id | string -%}

    {%- if ns.has_rsrc_static_defined -%}
        {%- set rsrc_statics = ns.source_models_rsrc_dict[source_number|string] -%}
    {%- endif -%}

    {%- if 'hk_column' not in source_model.keys() %}
        {%- set hk_column = tracked_hashkey -%}
    {%- else -%}
        {%- set hk_column = source_model['hk_column'] -%}
    {% endif %}

    src_new_{{ source_number }} AS (

        SELECT
            {{ hk_column }} AS {{ tracked_hashkey }},
            {{ src_ldts }},
            {{ src_rsrc }},
            CAST(UPPER('{{ source_model.name }}') AS {{ stg_default_dtype }}) AS {{ src_stg }}
            {% for col in additional_columns -%}
            ,{{ col }}
            {% endfor -%}
        FROM {{ ref(source_model.name) }} src
        {{ log('rsrc_statics defined?: ' ~ ns.source_models_rsrc_dict[source_number|string], false) }}

    {%- if is_incremental() and ns.has_rsrc_static_defined and ns.source_included_before[source_number|int] and not disable_hwm %}
        INNER JOIN max_ldts_per_rsrc_static_in_target max ON
        ({%- for rsrc_static in rsrc_statics -%}
            max.rsrc_static = '{{ rsrc_static }}'
            {%- if not loop.last -%} OR
            {% endif -%}
        {%- endfor %})
        WHERE src.{{ src_ldts }} > max.max_ldts
    {%- elif is_incremental() and source_models | length == 1 and not ns.has_rsrc_static_defined and not disable_hwm %}
        WHERE src.{{ src_ldts }} > (
            SELECT MAX({{ src_ldts }})
            FROM {{ this }}
            WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
            )
    {%- endif %}

         {%- set ns.last_cte = "src_new_{}".format(source_number) %}

    ),
{%- endfor -%}

{%- if source_models | length > 1 %}

source_new_union AS (

    {%- for source_model in source_models -%}

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

records_to_insert AS (

    SELECT
    {{ datavault4dbt.print_list(final_columns_to_select) }}
    FROM {{ ns.last_cte }}
    WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }} 
    AND {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }}
    {%- if is_incremental() %}
        {%- set concat_columns = [tracked_hashkey, src_ldts, src_rsrc] -%}
        AND NOT EXISTS(SELECT 1 from distinct_concated_target where {{ datavault4dbt.concat_ws(concat_columns) }} = distinct_concated_target.concat)
    {% endif %}
)

SELECT * FROM records_to_insert

{%- endmacro -%}
