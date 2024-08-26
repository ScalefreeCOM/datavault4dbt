{%- macro exasol__ref_hub(ref_keys, src_ldts, src_rsrc, source_models) -%}

{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- set ns = namespace(last_cte= "", source_included_before = {}, has_rsrc_static_defined=true, source_models_rsrc_dict={}) -%}

{%- set ref_keys = datavault4dbt.expand_column_list(columns=[ref_keys]) -%}

{# If no specific ref_keys is defined for each source, we apply the values set in the ref_keys variable. #}
{# If no rsrc_static parameter is defined in ANY of the source models then the whole code block of record_source performance lookup is not executed  #}
{# For the use of record_source performance lookup it is required that every source model has the parameter rsrc_static defined and it cannot be an empty string #}
{%- if source_models is not mapping and not datavault4dbt.is_list(source_models) -%}
    {%- set source_models = {source_models: {}} -%}
{%- endif -%}

{%- set source_model_values = fromjson(datavault4dbt.source_model_processing(source_models=source_models, parameters={'test':'test'}, reference_keys=ref_keys)) -%}
{%- set source_models = source_model_values['source_model_list'] -%}
{%- set ns.has_rsrc_static_defined = source_model_values['has_rsrc_static_defined'] -%}
{%- set ns.source_models_rsrc_dict = source_model_values['source_models_rsrc_dict'] -%}
{{ log('source_models: '~source_models, false) }}

{%- set final_columns_to_select = ref_keys + [src_ldts] + [src_rsrc] -%}

{{ datavault4dbt.prepend_generated_by() }}

WITH

{% if is_incremental() -%}
{# Get all target ref_keys out of the existing ref_table for later incremental logic. #}
    distinct_target_ref_keys AS (

        SELECT
            {{ datavault4dbt.concat_ws(ref_keys) }} as c
        FROM {{ this }}

    ),
    {%- if ns.has_rsrc_static_defined -%}
        {% for source_model in source_models %}
         {# Create a query with a rsrc_static column with each rsrc_static for each source model. #}
            {%- set source_number = source_model.id | string -%}
            {%- set rsrc_statics = ns.source_models_rsrc_dict[source_number] -%}

            {{log('rsrc_statics: '~ rsrc_statics, false) }}

            {%- set rsrc_static_query_source -%}
                SELECT count(*) FROM (
                {%- for rsrc_static in rsrc_statics -%}
                    SELECT t.{{ src_rsrc }},
                    '{{ rsrc_static }}' AS rsrc_static
                    FROM {{ this }} t
                    WHERE {{ src_rsrc }} like '{{ rsrc_static }}'
                    {%- if not loop.last %}
                        UNION ALL
                    {% endif -%}
                {%- endfor -%}
                )
            {% endset %}

            rsrc_static_{{ source_number }} AS (
                {%- for rsrc_static in rsrc_statics -%}
                    SELECT 
                    t.{{ src_ldts }},
                    '{{ rsrc_static }}' AS rsrc_static
                    FROM {{ this }} t
                    WHERE {{ src_rsrc }} LIKE '{{ rsrc_static }}'
                    {%- if not loop.last %}
                        UNION ALL
                    {% endif -%}
                {%- endfor -%}
                {%- set ns.last_cte = "rsrc_static_{}".format(source_number) -%}
            ),

            {%- set source_in_target = true -%}

            {%- if execute -%}
                {%- set rsrc_static_result = run_query(rsrc_static_query_source) -%}

                {%- set row_count = rsrc_static_result.columns[0].values()[0] -%}

                {{ log('row_count for '~source_model~' is '~row_count, false) }}

                {%- if row_count == 0 -%}
                {%- set source_in_target = false -%}
                {%- endif -%}
            {%- endif -%}


            {%- do ns.source_included_before.update({source_model.id: source_in_target}) -%}

        {% endfor -%}

        {%- if source_models | length > 1 %}

        rsrc_static_union AS (
            {#  Create one unionized table over all sources. It will be the same as the already existing
                hub, but extended by the rsrc_static column. #}
            {% for source_model in source_models %}
            {%- set source_number = source_model.id | string -%}

            SELECT rsrc_static_{{ source_number }}.* FROM rsrc_static_{{ source_number }}

            {%- if not loop.last %}
            UNION ALL
            {% endif -%}
            {%- endfor %}
            {%- set ns.last_cte = "rsrc_static_union" -%}
        ),

        {%- endif %}

        max_ldts_per_rsrc_static_in_target AS (
        {# Use the previously created CTE to calculate the max load date timestamp per rsrc_static. #}
            SELECT
                rsrc_static,
                MAX({{ src_ldts }}) as max_ldts
            FROM {{ ns.last_cte }}
            WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
            GROUP BY rsrc_static

        ),
    {%- endif %}
{% endif -%}

{% for source_model in source_models %}

    {%- set source_number = source_model.id | string -%}

    {%- if ns.has_rsrc_static_defined -%}
        {%- set rsrc_statics = ns.source_models_rsrc_dict.id -%}
    {%- endif -%}


    src_new_{{ source_number }} AS (

        SELECT
            {% for ref_key in source_model['ref_keys'] -%}
            {{ ref_key}},
            {% endfor -%}

            {{ src_ldts }},
            {{ src_rsrc }}
        FROM {{ ref(source_model.name) }} src
        WHERE NOT (
            {% for ref_key in source_model['ref_keys'] -%}
            {{ ref_key}} IS NULL {%- if not loop.last %} AND {% endif -%}
            {% endfor -%} )

    {%- if is_incremental() and ns.has_rsrc_static_defined and ns.source_included_before[source_number] %}
        INNER JOIN max_ldts_per_rsrc_static_in_target max ON
        ({%- for rsrc_static in rsrc_statics -%}
            max.rsrc_static = '{{ rsrc_static }}'
            {%- if not loop.last -%} OR
            {% endif -%}
        {%- endfor %})
        AND src.{{ src_ldts }} > max.max_ldts
    {%- endif %}

         {%- set ns.last_cte = "src_new_{}".format(source_number) %}

    ),
{%- endfor -%}

{%- if source_models | length > 1 %}

source_new_union AS (

    {%- for source_model in source_models -%}

    {%- set source_number = source_model.id | string -%}

    SELECT
        {% for ref_key in source_model['ref_keys'] -%}
            {{ ref_key }} AS {{ ref_keys[loop.index - 1] }},
        {% endfor -%}

        {{ src_ldts }},
        {{ src_rsrc }}
    FROM src_new_{{ source_number }}

    {%- if not loop.last %}
    UNION ALL
    {% endif -%}

    {%- endfor -%}

    {%- set ns.last_cte = 'source_new_union' -%}

),

{%- endif %}

earliest_ref_key_over_all_sources AS (

    {#- Deduplicate the unionized records to only insert the earliest one. #}
    SELECT
        lcte.*
    FROM {{ ns.last_cte }} AS lcte

    QUALIFY ROW_NUMBER() OVER (PARTITION BY {%- for ref_key in ref_keys %} {{ref_key}} {%- if not loop.last %}, {% endif %}{% endfor %} ORDER BY {{ src_ldts }}) = 1

    {%- set ns.last_cte = 'earliest_ref_key_over_all_sources' -%}

),

records_to_insert AS (
    {#- Select everything from the previous CTE, if incremental filter for hashkeys that are not already in the hub. #}
    SELECT
        {{ datavault4dbt.print_list(final_columns_to_select) }}
    FROM {{ ns.last_cte }}

    {%- if is_incremental() %}
    WHERE {{ datavault4dbt.concat_ws(ref_keys) }} NOT IN (SELECT * FROM distinct_target_ref_keys)
    {% endif -%}
)

SELECT * FROM records_to_insert

{%- endmacro -%}
