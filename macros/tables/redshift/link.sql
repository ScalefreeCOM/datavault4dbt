
{%- macro redshift__link(link_hashkey, foreign_hashkeys, source_models, src_ldts, src_rsrc, disable_hwm, additional_columns) -%}

{%- if not (foreign_hashkeys is iterable and foreign_hashkeys is not string) -%}

    {%- if execute -%}
        {{ exceptions.raise_compiler_error("Only one foreign key provided for this link. At least two required.") }}
    {%- endif %}

{%- endif -%}

{%- set ns = namespace(last_cte= "", source_included_before = {}, has_rsrc_static_defined=true, source_models_rsrc_dict={}) -%}

{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{# Select the additional_columns from the link model and put them in an array. If additional_colums none, then empty array #}
{%- set additional_columns = additional_columns | default([],true) -%}
{%- set additional_columns = [additional_columns] if additional_columns is string else additional_columns -%}

{# If no specific link_hk and fk_columns are defined for each source, we apply the values set in the link_hashkey and foreign_hashkeys variable. #}
{# If no rsrc_static parameter is defined in ANY of the source models then the whole code block of record_source performance lookup is not executed  #}
{# For the use of record_source performance lookup it is required that every source model has the parameter rsrc_static defined and it cannot be an empty string #}
{%- if source_models is not mapping and not datavault4dbt.is_list(source_models) -%}
    {%- set source_models = {source_models: {}} -%}
{%- endif -%}

{%- set source_model_values = fromjson(datavault4dbt.source_model_processing(source_models=source_models, parameters={'link_hk':link_hashkey}, foreign_hashkeys=foreign_hashkeys)) -%}
{%- set source_models = source_model_values['source_model_list'] -%}
{%- set ns.has_rsrc_static_defined = source_model_values['has_rsrc_static_defined'] -%}
{%- set ns.source_models_rsrc_dict = source_model_values['source_models_rsrc_dict'] -%}
{{ log('source_models: '~source_models, false) }}

{%- set final_columns_to_select = [link_hashkey] + foreign_hashkeys + [src_ldts] + [src_rsrc] + additional_columns  -%}

{{ datavault4dbt.prepend_generated_by() }}


{{ 'with source_models as (' if source_models | length > 1 }}

{%- for source_model in source_models -%}
{# Loop over all source models and select the data #}

    {%- if 'link_hk' not in source_model.keys() -%}
        {%- set link_hk = link_hashkey -%}
    {%- else -%}
        {%- set link_hk = source_model['link_hk'] -%}
    {%- endif %}

    {%- set source_number = source_model.id | string -%}

    {%- if ns.has_rsrc_static_defined -%}
        {%- set rsrc_statics = ns.source_models_rsrc_dict[source_number|string] -%}
    {%- else -%}
        {%- set rsrc_statics = ['dummy_entry_for_the_for_loop'] -%}
    {%- endif -%}

    {%- for rsrc_static in rsrc_statics %}
    {# To avoid expensive OR in the where condition if multiple rsrc_static values are defined we create a new statement for each and UNION #}

    SELECT
        {{ link_hk }} AS {{ link_hashkey }},
        {% for fk in source_model['fk_columns'] -%}
        {{ fk }},
        {% endfor -%}

        {% for col in additional_columns -%}
        {{ col }},
        {% endfor -%}

        {{ src_ldts }},
        {{ src_rsrc }}
    FROM {{ ref(source_model.name) }} src
    WHERE 1=1
      {% if is_incremental() and ns.has_rsrc_static_defined and not disable_hwm -%}
        AND 
            (src.{{ src_ldts }} > (
                select MAX({{ src_ldts }}) AS {{ src_ldts }} FROM {{ this }}
                WHERE {{ src_rsrc }} LIKE '{{ rsrc_static }}' AND {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }})
            AND src.{{ link_hk }} NOT IN (
                select {{ link_hashkey }} FROM {{ this }} WHERE 1=1 {{ datavault4dbt.filter_distinct_target_hashkey_in_link(src_rsrc = src_rsrc, rsrc_static = rsrc_static) }})
            AND src.{{ src_rsrc }} LIKE '{{ rsrc_static }}')
      {%- elif is_incremental() and not ns.has_rsrc_static_defined and not disable_hwm %}
        AND src.{{ src_ldts }} > (
            select MAX({{ src_ldts }}) AS {{ src_ldts }} FROM {{ this }} WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }})
        AND src.{{ link_hk }} NOT IN (select {{ link_hashkey }} FROM {{ this }} WHERE 1=1 {{ datavault4dbt.filter_distinct_target_hashkey_in_link() }})
      {%- elif is_incremental() %}
        AND src.{{ link_hk }} NOT IN (select {{ link_hashkey }} FROM {{ this }} WHERE 1=1 {{ datavault4dbt.filter_distinct_target_hashkey_in_link() }})
      {%- endif %}

    {{ 'UNION' if not loop.last }} {# To Union multiple rsrc_statics #}
    {%- endfor -%}
    {{ 'UNION' if not loop.last }} {# To Union multiple source models #}


    {%- if source_models | length == 1 %}
    QUALIFY ROW_NUMBER() over (PARTITION BY {{ link_hk }} ORDER BY {{ src_ldts }} ASC) = 1
    {% endif -%}
{%- endfor -%}

{%- if source_models | length > 1 %}
)

select * from source_models sm
QUALIFY ROW_NUMBER() over (PARTITION BY {{ link_hashkey }} ORDER BY {{ src_ldts }} ASC) = 1
{% endif -%}

{% endmacro %}