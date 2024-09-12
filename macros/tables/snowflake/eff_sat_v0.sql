{%- macro snowflake__eff_sat_v0(source_models, tracked_hashkey, src_ldts, src_rsrc, deleted_flag_alias) -%}

{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- set ns = namespace(new_hashkeys_cte="", disappeared_hashkeys_cte="", last_cte= "", source_included_before = {}, has_rsrc_static_defined=true, source_models_rsrc_dict={}) -%}

{# If no rsrc_static parameter is defined in ANY of the source models then the whole code block of record_source performance lookup is not executed  #}
{# For the use of record_source performance lookup it is required that every source model has the parameter rsrc_static defined and it cannot be an empty string #}
{%- if source_models is not mapping and not datavault4dbt.is_list(source_models) -%}
    {%- set source_models = {source_models: {}} -%}
{%- endif -%}

{%- set source_model_values = fromjson(datavault4dbt.source_model_processing(source_models=source_models, parameters={'tracked_hashkey':tracked_hashkey})) -%}
{%- set source_models = source_model_values['source_model_list'] -%}
{%- set ns.has_rsrc_static_defined = source_model_values['has_rsrc_static_defined'] -%}
{%- set ns.source_models_rsrc_dict = source_model_values['source_models_rsrc_dict'] -%}
{{ log('source_models: '~source_models, false) }}

{%- set final_columns_to_select = [tracked_hashkey] + [src_rsrc]  + [src_ldts] + [deleted_flag_alias] -%}

{%- set final_columns_to_select = datavault4dbt.escape_column_names(final_columns_to_select) -%}
{%- set tracked_hashkey = datavault4dbt.escape_column_names(tracked_hashkey) -%}
{%- set deleted_flag_alias = datavault4dbt.escape_column_names(deleted_flag_alias) -%}
{%- set src_ldts = datavault4dbt.escape_column_names(src_ldts) -%}
{%- set src_rsrc = datavault4dbt.escape_column_names(src_rsrc) -%}

{{ log('columns to select: '~final_columns_to_select, false) }}

{{ datavault4dbt.prepend_generated_by() }}

WITH 
{#
    For incremental runs, three different cases can occur: 
        hashkey disappeared ->  deleted_flag = 1
        hashkey reappeared  ->  deleted_flag = 0
        new hashkey appeared -> deleted_flag = 0
#}
{% if is_incremental() and execute %}

    {#
        First, the current status for each hashkey is queried
    #}
    current_status AS (

        SELECT
            {{ tracked_hashkey }},
            {{ deleted_flag_alias }},
            {{ src_rsrc }}
        FROM {{ this }}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ tracked_hashkey }} ORDER BY {{ src_ldts }} desc) = 1

    ),

    {% for source_model in source_models -%}

    	{#
            For each source_model, all hashkeys that are not yet in the effectivity satellite, or are currently marked as deleted, get 0 as deleted_flag.
        #}
        {%- set source_number = source_model.id | string -%}
        {%- set tracked_hashkey_src = datavault4dbt.escape_column_names(source_model['tracked_hashkey']) -%}

        new_hashkeys_{{ source_number }} AS (

            SELECT DISTINCT 
                src.{{ tracked_hashkey_src }} AS {{ tracked_hashkey }},
                src.{{ src_rsrc }},
                {{ datavault4dbt.current_timestamp() }} as {{ src_ldts }},
                0 as {{ deleted_flag_alias }}
            FROM {{ ref(source_model.name) }} src
            LEFT JOIN current_status cs
                ON src.{{ tracked_hashkey }} = cs.{{ tracked_hashkey }}
                AND cs.{{ deleted_flag_alias }} = 0
            WHERE cs.{{ tracked_hashkey }} IS NULL

            {%- set ns.new_hashkeys_cte = 'new_hashkeys_' ~ source_number -%}

        ),

    {%- endfor %}

        {#
            All hashkeys, that currently have deleted_flag = 0, are searched for in all source models. 
            If they can't be found anywhere, they are marked as deleted_flag = 1.
        #}
        disappeared_hashkeys AS (

            SELECT DISTINCT 
                cs.{{ tracked_hashkey }},
                cs.{{ src_rsrc }},
                {{ datavault4dbt.current_timestamp() }} as {{ src_ldts }},
                1 as {{ deleted_flag_alias }}
            FROM current_status cs
            WHERE 
            {% for source_model in source_models %}
                {%- set tracked_hashkey_src = datavault4dbt.escape_column_names(source_model['tracked_hashkey']) -%}
                {{ 'AND' if not loop.first }}
                NOT EXISTS (
                    SELECT 
                        1 
                    FROM {{ ref(source_model.name) }} src
                    WHERE src.{{ tracked_hashkey_src }} = cs.{{ tracked_hashkey }}
                )
            {% endfor %}
            AND cs.{{ deleted_flag_alias }} = 0

        ),


    {%- if source_models | length > 1 -%}

        {#
            If more then one source_model is defined, the new hashkeys of all source_models are unioned.
        #}
        new_hashkeys_union AS (

            {%- for source_model in source_models -%}

            {%- set source_number = source_model.id | string -%}

            SELECT
                {{ tracked_hashkey }},
                {{ src_rsrc }},
                {{ src_ldts }},
                {{ deleted_flag_alias }}
            FROM new_hashkeys_{{ source_number }}

            {%- if not loop.last %}
            UNION
            {% endif -%}

            {%- endfor -%}

        ),

        new_hashkeys_union_dedupe AS (

            SELECT
                {{ tracked_hashkey }},
                {{ src_rsrc }},
                {{ src_ldts }},
                {{ deleted_flag_alias }}
            FROM new_hashkeys_union
            QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ tracked_hashkey }} ORDER BY {{ src_ldts }}) = 1

            {%- set ns.last_cte = 'new_hashkeys_union_dedupe' -%}

        ),       

    {%- endif %}

    {#
        All hashkeys that have a status change  should be inserted. 
        That includes new, reappeared, and disappeared hashkeys. 
    #}
    records_to_insert AS (

        SELECT
            {{ datavault4dbt.print_list(final_columns_to_select) }}
        FROM {{ ns.new_hashkeys_cte }}

        UNION 

        SELECT
            {{ datavault4dbt.print_list(final_columns_to_select) }}
        FROM disappeared_hashkeys

    )

{%- else %}

    {#
        In initial runs, every available hashkey is marked as deleted_flag = 0.
    #}
    {% for source_model in source_models %}

        {#
            When multiple source_models are defined, the hashkeys of all source_models are determined and unioned.
        #}
        {%- set source_number = source_model.id | string -%}
        {%- set tracked_hashkey_src = datavault4dbt.escape_column_names(source_model['tracked_hashkey']) -%}

        hashkeys_{{ source_number }} AS (

            SELECT DISTINCT 
                src.{{ tracked_hashkey_src }} AS {{ tracked_hashkey }},
                src.{{ src_rsrc }},
                {{ datavault4dbt.current_timestamp() }} as {{ src_ldts }},
                0 as {{ deleted_flag_alias }}
            FROM {{ ref(source_model.name) }} src

            {%- set ns.last_cte = 'hashkeys_' ~ source_number -%}

        ),

    {%- endfor %}

    {% if source_models | length > 1 -%}

        hashkeys_union AS (

            {%- for source_model in source_models -%}

            {%- set source_number = source_model.id | string -%}

            SELECT
                {{ tracked_hashkey }},
                {{ src_rsrc }},
                {{ src_ldts }},
                {{ deleted_flag_alias }}
            FROM hashkeys_{{ source_number }}

            {%- if not loop.last %}
            UNION 
            {% endif -%}

            {%- endfor -%}


        ),

        hashkey_union_dedupe AS (

            SELECT
                {{ tracked_hashkey }},
                {{ src_rsrc }},
                {{ src_ldts }},
                {{ deleted_flag_alias }}
            FROM hashkeys_union
            QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ tracked_hashkey }} ORDER BY {{ src_ldts }}) = 1

            {%- set ns.last_cte = 'hashkey_union_dedupe' -%}

        ),

    {%- endif %}

    records_to_insert AS (

        SELECT
            {{ datavault4dbt.print_list(final_columns_to_select) }}
        FROM {{ ns.last_cte }}        

    )


{% endif %}

SELECT 
    {{ datavault4dbt.print_list(final_columns_to_select) }}
FROM records_to_insert

{%- endmacro -%}
