{#-
    This macro creates Effectivity Satellites attached to Links.
    Its needed for detecting deletes, when that information is not provided by CDC.

    Parameters:
    - link::string                  Name of the link model that this eff sat should be attached to
    - src_pk::string                Name of the Primary Key column inside the Link. Usually the Link Hashkey
    - src_ldts::string              Name of the LoadDate column inside the Link. Usually 'ldts'
    - src_rsrc::string              Name of the RecordSource column inside the Link. Usually 'rsrc'
    - source_model::list or string  Name(s) of the source data models. Usually the staging layer
-#}

{%- macro eff_sat_link(link, src_pk, src_ldts, src_rsrc, source_model) -%}

    {{ adapter.dispatch('eff_sat_link', 'dbtvault_scalefree')(link=link, 
                                                  src_pk=src_pk,
                                                  src_ldts=src_ldts,
                                                  src_rsrc=src_rsrc,
                                                  source_model=source_model) }}

{%- endmacro -%}  


{%- macro default__eff_sat_link(link,), src_pk, src_ldts, src_rsrc, source_model) -%}

{{- dbtvault_scalefree.check_required_parameters(hub=hub, 
                                                 src_pk=src_pk,
                                                 src_ldts=src_ldts, 
                                                 src_rsrc=src_rsrc,
                                                 source_model=source_model) -}}

{%- set source_cols = dbtvault_scalefree.expand_column_list(columns=[src_pk, src_ldts, src_rsrc]) -%}

{#- Select hashing algorithm -#}
{%- set hash = var('dbtvault_scalefree.hash', 'MD5') -%}
{%- set hash_alg, unknown_key, error_key = dbtvault_scalefree.hash_default_values(hash_function=hash) -%}

{%- set beginning_of_all_times = var('dbtvault_scalefree.beginning_of_all_times', '0001-01-01T00-00-01') -%}
{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}

{{ prepend_generated_by() }}

WITH

{%- if not (source_model is iterable and source_model is not string) -%}
    {%- set source_model = [source_model] -%}
{%- endif -%}

{#- Get available HKs from stage #}
source_union AS (
    {% for src in source_model -%}
    SELECT 
        {{ dbtvault_scalefree.prefix(source_cols, 'source') }}
    FROM 
        {{ ref(src) }} source
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor -%}
    ),

{#- Get Link data #}
target_link AS (
    SELECT 
        {{ dbtvault_scalefree.prefix(source_cols, 'link') }}
    FROM 
        {{ ref(link) }} link
    WHERE {{ src_pk }} != '{{ zero_key }}'
    AND {{ src_pk }} != '{{ error_key }}'
    ),

{#- Get the latest record per PK from existing esat in incremental runs #}
{%- if is_incremental() %}
esat_latest AS (
    SELECT *
    FROM  {{ this }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ src_pk }} ORDER BY {{ src_ldts }} DESC) = 1
    ),
{%- endif %}

{#- Get Link entries with PK, that are no longer available in stage #}
deleted AS (
    SELECT {{ dbtvault_scalefree.prefix(source_cols, 'target_link') }}
    FROM target_link
    WHERE {{ src_pk }} NOT IN (
        SELECT DISTINCT {{ src_pk }}
        FROM src_union
    )
    ),

{# Records that already were available and still are available, get deleted=false #}
still_active AS (
        SELECT {{ dbtvault_scalefree.prefix(source_cols, 'target_hub') }}
            , {{ src_ldts }} AS start_dts
            , PARSE_TIMESTAMP('{{ timestamp_format }}', '{{ end_of_all_times }}') AS end_dts
            , false AS is_deleted 
        FROM target_hub
        {%- if is_incremental() %}
        WHERE {{ src_pk }} NOT IN (
            SELECT {{ src_pk }}
            FROM esat_latest
        )
        {%- endif %}
    )

{# Records that were available but are now unavailable, get deleted=true #}
    , now_unactive AS (
        SELECT
            {{ src_pk }}
            , CURRENT_TIMESTAMP() AS {{ src_ldts }}
            , {{ src_rsrc }}
            , {{ src_ldts }} AS start_dts
            , CURRENT_TIMESTAMP() AS end_dts
            , true AS is_deleted
        FROM deleted
        {%- if is_incremental() %}
        WHERE {{ src_pk }} NOT IN (
            SELECT {{ src_pk }}
            FROM esat_latest
            WHERE is_deleted
        )
        {%- endif %}
    )
    
{# Records that were unactive before, but are now reactivated, get deleted=false again #}
{%- if is_incremental() %}
    , reactivated AS (
        SELECT {{ dbtvault_scalefree.prefix(source_cols, 'source_union') }}
            , {{ src_ldts }} as start_dts
            , PARSE_TIMESTAMP('{{ timestamp_format }}', '{{ end_of_all_times }}') AS end_dts
            , false as is_deleted 
        FROM source_union
        WHERE {{ src_pk }} IN (
            SELECT {{ src_pk }}
            FROM esat_latest
            WHERE is_deleted
        )
    )
{%- endif -%}

{# Combine all records that need to be inserted #}
    , records_to_insert AS (
        SELECT * FROM still_active
        UNION ALL
        SELECT * FROM now_unactive
        {% if is_incremental() %}
        UNION ALL
        SELECT * FROM reactivated
        {%- endif %}
    )

SELECT * FROM records_to_insert

{%- endmacro -%}


{%- macro eff_sat_link_v0(target_entity,
                          hashkey_column,
                          source_model,
                          src_ldts,
                          src_rsrc) -%}

{%- set ns = namespace(last_cte= "") -%}

{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}

{%- if not (source_model is mapping) -%}
    {{ exceptions.raise_compiler_error("Invalid Source Model definition. Needs to be a dictionary, with all source models as keys,  and for each source model a dictionary as value, having the keys 'rsrc_static' and optionally 'hk_column'.") }}
{%- endif -%}     

{%- for source in source_model.keys() -%}

    {%- if 'hk_column' not in source_model[source].keys() -%}
        {%- do source_model[source].update({'hk_column': hashkey_column}) -%}
        {{ log("No custom hashkey column defined for source model '" + source + ", using default hashkey column '" + hashkey_column + " instead.", true) }}
    {%- endif -%}

{%- endfor -%}

{# Incremental: Get latest ldts per rsrc_static in existing target_entity #}

{%- if is_incremental() -%}

{%- for source in source_model.keys() %}

    {%- set source_number = loop.index | string -%}
    {%- set rsrc_static = source_model[source]['rsrc_static'] -%}

    rsrc_static_{{ source_number }} AS (

        SELECT
            *, 
            '{{ rsrc_static }}' AS rsrc_static
        FROM {{ this }}
        WHERE {{ src_rsrc }} like '{{ rsrc_static }}'

        {%- set ns.last_cte = "rsrc_static_{}".format(source_number) -%}
    
    ),
{% endfor -%}

{%- if source_model.keys() | length > 1 %}

rsrc_static_union AS (

    {% for source in source_model.keys() %}
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

    SELECT
        rsrc_static,
        MAX({{ src_ldts }}) as max_ldts
    FROM {{ ns.last_cte }}
    WHERE {{ src_ldts }} != {{ dbtvault_scalefree.string_to_timestamp(timestamp_format, end_of_all_times) }}
    GROUP BY rsrc_static

), 
{% endif -%} {# End if_incremental #}

{# Selecting each source, if incremental only the new ldts in each source. #}
{%- for source in source_model.keys() -%}

    {%- set source_number = loop.index | string -%}
    {%- set rsrc_static = source_models[source_model]['rsrc_static'] -%}
    {%- set hk_column = source_models[source_model]['hk_column'] -%}

    src_{{ source_number }} AS (

        SELECT
            {{ hk_column }} AS {{ hashkey_column }}, 
            {{ src_ldts }},
            {{ src_rsrc }},
            '{{ rsrc_static }}' AS rsrc_static
        FROM {{ ref(source) }} src

        {%- if is_incremental() %}
        INNER JOIN max_ldts_per_rsrc_static_in_target max
            ON max.rsrc_static = '{{ rsrc_static }}'
        WHERE src.{{ src_ldts }} > max.max_ldts
        {%- endif -%}

        {%- set ns.last_cte = "src_{}".format(source_number) %}

    ),
{%- endfor -%}

{# Unionize all sources. #}
{%- if source_model.keys() | length > 1 %}

source_union AS (

    {%- for source in source_model.keys() -%}

    {%- set source_number = loop.index | string -%}

    SELECT
        {{ hashkey_column }},
        {{ src_ldts }},
        {{ src_rsrc }},
        rsrc_static
    FROM src_{{ source_number }}

    {%- if not loop.last %}
    UNION ALL
    {% endif -%}

    {%- endfor -%}

    {%- set ns.last_cte = 'source_union' -%}

),
{%- endif -%}

distinct_source_rows AS (

    SELECT 
        {{ hashkey_column }},
        {{ src_ldts }},
        MAX({{ src_rsrc }}) as {{ src_rsrc }},
        MAX('rsrc_static') as rsrc_static
    FROM {{ ns.last_cte }}
    GROUP BY {{ hashkey_column }}, {{ src_ldts }}

    {%- set ns.last_cte = 'distinct_source_rows' -%}

),

distinct_loads AS (

    SELECT DISTINCT
        {{ src_ldts }}
    FROM {{ ns.last_cte }}

),

next_distinct_loads AS (

    SELECT 
        {{ src_ldts }},
        COALESCE(LEAD({{ src_ldts }}) OVER (ORDER BY {{ src_ldts }}), dbtvault_scalefree.string_to_timestamp(format=timestamp_format, timestamp=end_of_all_times)) as next_load,
    FROM distinct_loads

),

actual_next_loads AS (

    SELECT 
        {{ hashkey_column }},
        {{ src_ldts }},
        {{ src_rsrc }},
        COALESCE(LEAD({{ src_ldts }}) OVER (PARTITION BY {{ hashkey_column }} ORDER BY {{ src_ldts }}), dbtvault_scalefree.string_to_timestamp(format=timestamp_format, timestamp=end_of_all_times)) as next_load
    FROM {{ ns.last_cte }}

),

sdfsdf AS (

    SELECT 
        {{ hashkey_column }},
        actual_next_loads.{{ src_ldts }},
        CASE 
            WHEN actual_next_loads.next_load > next_distinct_loads.next_load THEN 

    FROM actual_next_loads 
    JOIN next_distinct_loads ON actual_next_loads.{{ src_ldts }} = next_distinct_loads.{{ src_ldts }} 
    WHERE 
)

 {%- endmacro -%}