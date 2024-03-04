{%- macro redshift__nh_link(link_hashkey, foreign_hashkeys, payload, source_models, src_ldts, src_rsrc, disable_hwm, source_is_single_batch) -%}
{%- if not (foreign_hashkeys is iterable and foreign_hashkeys is not string) -%}

    {%- if execute -%}
        {{ exceptions.raise_compiler_error("Only one foreign key provided for this link. At least two required.") }}
    {%- endif %}

{%- endif -%}
{%- set ns = namespace(last_cte= "", source_included_before = {}, has_rsrc_static_defined=true, source_models_rsrc_dict={}) -%}

{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}


{# If no specific link_hk, fk_columns, or payload are defined for each source, we apply the values set in the link_hashkey, foreign_hashkeys, and payload variable. #}
{# If no rsrc_static parameter is defined in ANY of the source models then the whole code block of record_source performance lookup is not executed  #}
{# For the use of record_source performance lookup it is required that every source model has the parameter rsrc_static defined and it cannot be an empty string #}
{%- if source_models is not mapping and not datavault4dbt.is_list(source_models) -%}
    {%- set source_models = {source_models: {}} -%}
{%- endif -%}

{%- set source_model_values = fromjson(datavault4dbt.source_model_processing(source_models=source_models, parameters={'link_hk':link_hashkey}, foreign_hashkeys=foreign_hashkeys, payload=payload)) -%}
{%- set source_models = source_model_values['source_model_list'] -%}
{%- set ns.has_rsrc_static_defined = source_model_values['has_rsrc_static_defined'] -%}
{%- set ns.source_models_rsrc_dict = source_model_values['source_models_rsrc_dict'] -%}
{{ log('source_models: '~source_models, false) }}

{%- set final_columns_to_select = [link_hashkey] + foreign_hashkeys + [src_ldts] + [src_rsrc] + payload -%}

{{ datavault4dbt.prepend_generated_by() }}

WITH

{%- if is_incremental() -%}
{# Get all link hashkeys out of the existing link for later incremental logic. #}
    distinct_target_hashkeys AS (

        SELECT
        {{ link_hashkey }}
        FROM {{ this }}

    ),
    {%- if ns.has_rsrc_static_defined and not disable_hwm -%}
        {% for source_model in source_models %}
        {# Create a query with a rsrc_static column with each rsrc_static for each source model. #}
            {%- set source_number = source_model.id | string -%}
            {%- set rsrc_statics = ns.source_models_rsrc_dict[source_number] -%}

            {{log('rsrc_statics: '~ rsrc_statics, false) }}

            {%- set rsrc_static_query_source -%}
                SELECT count(*) FROM (
                {%- for rsrc_static in rsrc_statics -%}
                    SELECT t.{{ src_rsrc }},
                    CAST('{{ rsrc_static }}' AS VARCHAR) AS rsrc_static
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
                    SELECT t.*,
                    CAST('{{ rsrc_static }}' AS VARCHAR) AS rsrc_static
                    FROM {{ this }} t
                    WHERE {{ src_rsrc }} like '{{ rsrc_static }}'
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
                nh_link, but extended by the rsrc_static column. #}

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
                MAX({{ src_ldts }}) AS max_ldts
            FROM {{ ns.last_cte }}
            WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
            GROUP BY rsrc_static

        ),
    {%- endif %}
{% endif -%}

{% for source_model in source_models %}

{#  Select all deduplicated records from each source, and filter for records that are newer
    than the max ldts inside the existing link, if incremental. #}

    {%- set source_number = source_model.id | string -%}

    {%- if ns.has_rsrc_static_defined -%}
        {%- set rsrc_statics = ns.source_models_rsrc_dict[source_number|string] -%}
    {%- endif -%}

    {%- if 'link_hk' not in source_model.keys() %}
        {%- set link_hk = link_hashkey -%}
    {%- else -%}
        {%- set link_hk = source_model['link_hk'] -%}
    {% endif %}

src_new_{{ source_number }} AS (

    SELECT
            {{ link_hk }} AS {{ link_hashkey }},
            {% for fk in source_model['fk_columns'] -%}
            {{ fk }},
            {% endfor -%}
        {{ src_ldts }},
        {{ src_rsrc }},

        {{ datavault4dbt.print_list(source_model['payload']) | indent(3) }}

    FROM {{ ref(source_model.name) }} src
    {# If the model is incremental and all sources has rsrc_static defined and valid and the source was already included before in the target transactional link #}
    {# then an inner join is performed on the CTE for the maximum load date timestamp per record source static to get the records
    that match any of the rsrc_static present in it #}
    {# if there are records in the source with a newer load date time stamp than the ones present in the target, those will be selected to be inserted later #}
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
{# Unionize the new records from all sources. #}

    {%- for source_model in source_models -%}

    {%- set source_number = source_model.id | string -%}

    SELECT
        {{ link_hashkey }},
        {% for fk in source_model['fk_columns']|list %}
            {{ fk }} AS {{ foreign_hashkeys[loop.index - 1] }},
        {% endfor -%}

        {{ src_ldts }},
        {{ src_rsrc }},

        {% for col in source_model['payload']|list %}
            {{ col }} AS {{ payload[loop.index - 1] }}
            {%- if not loop.last %}, {%- endif %}
        {% endfor -%}

    FROM src_new_{{ source_number }}

    {%- if not loop.last %}
    UNION ALL
    {% endif -%}

    {%- endfor -%}

    {%- set ns.last_cte = 'source_new_union' -%}

),

{%- endif %}

{%- if not source_is_single_batch %}

earliest_hk_over_all_sources_prep AS (
    SELECT
        lcte.*,
        ROW_NUMBER() OVER (PARTITION BY {{ link_hashkey }} ORDER BY {{ src_ldts
        }}) as rn
    FROM {{ ns.last_cte }} AS lcte),

earliest_hk_over_all_sources AS (

    {#- Deduplicate the unionized records again to only insert the earliest one. #}
    SELECT
        lcte.*
    FROM earliest_hk_over_all_sources_prep AS lcte
        WHERE rn = 1
    {%- set ns.last_cte = 'earliest_hk_over_all_sources' -%}),

{%- endif %}

records_to_insert AS (
{# Select everything from the previous CTE, if its incremental then filter for hashkeys that are not already in the link. #}

    SELECT
        {{ datavault4dbt.print_list(final_columns_to_select) | indent(4) }}
    FROM {{ ns.last_cte }}

    {%- if is_incremental() %}
    WHERE NOT EXISTS (SELECT 1 FROM distinct_target_hashkeys 
                WHERE distinct_target_hashkeys.{{ link_hashkey }} = earliest_hk_over_all_sources.{{ link_hashkey }})
    {% endif %}
)

SELECT * FROM records_to_insert

{%- endmacro -%}
