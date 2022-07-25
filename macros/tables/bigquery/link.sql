{%- macro link(src_pk, src_fk, source_model, src_ldts='ldts', src_rsrc='rsrc') -%}

    {{- adapter.dispatch('link', 'dbtvault_scalefree')(src_pk=src_pk, src_fk=src_fk,
                                             src_ldts=src_ldts, src_rsrc=src_rsrc,
                                             source_model=source_model) -}}

{%- endmacro -%}

{%- macro default__link(src_pk, src_fk, src_ldts, src_rsrc, source_model) -%}

{{- dbtvault_scalefree.check_required_parameters(src_pk=src_pk, src_fk=src_fk,
                                       src_ldts=src_ldts, src_rsrc=src_rsrc,
                                       source_model=source_model) -}}

{%- set src_pk = dbtvault_scalefree.escape_column_names(src_pk) -%}
{%- set src_fk = dbtvault_scalefree.escape_column_names(src_fk) -%}
{%- set src_ldts = dbtvault_scalefree.escape_column_names(src_ldts) -%}
{%- set src_rsrc = dbtvault_scalefree.escape_column_names(src_rsrc) -%}

{%- set source_cols = dbtvault_scalefree.expand_column_list(columns=[src_pk, src_fk, src_ldts, src_rsrc]) -%}

{%- set fk_cols = dbtvault_scalefree.expand_column_list([src_fk]) -%}


{{ dbtvault_scalefree.prepend_generated_by() }}

{{ 'WITH ' -}}

{%- if not (source_model is iterable and source_model is not string) -%}
    {%- set source_model = [source_model] -%}
{%- endif -%}

{%- set ns = namespace(last_cte= "") -%}

{%- for src in source_model -%}

{%- set source_number = loop.index | string -%}

row_rank_{{ source_number }} AS (
    {%- if model.config.materialized == 'vault_insert_by_rank' %}
    SELECT {{ dbtvault_scalefree.prefix(source_cols_with_rank, 'rr') }},
    {%- else %}
    SELECT {{ dbtvault_scalefree.prefix(source_cols, 'rr') }},
    {%- endif %}
           ROW_NUMBER() OVER(
               PARTITION BY {{ dbtvault_scalefree.prefix([src_pk], 'rr') }}
               ORDER BY {{ dbtvault_scalefree.prefix([src_ldts], 'rr') }}
           ) AS row_number
    FROM {{ ref(src) }} AS rr
    QUALIFY row_number = 1
    {%- set ns.last_cte = "row_rank_{}".format(source_number) %}
),{{ "\n" if not loop.last }}
{% endfor -%}
{% if source_model | length > 1 %}
stage_union AS (
    {%- for src in source_model %}
    SELECT * FROM row_rank_{{ loop.index | string }}
    {%- if not loop.last %}
    UNION ALL
    {%- endif %}
    {%- endfor %}
    {%- set ns.last_cte = "stage_union" %}
),
{%- endif -%}
{%- if model.config.materialized == 'vault_insert_by_period' %}
stage_mat_filter AS (
    SELECT *
    FROM {{ ns.last_cte }}
    WHERE __PERIOD_FILTER__
    {%- set ns.last_cte = "stage_mat_filter" %}
),
{%- elif model.config.materialized == 'vault_insert_by_rank' %}
stage_mat_filter AS (
    SELECT *
    FROM {{ ns.last_cte }}
    WHERE __RANK_FILTER__
    {%- set ns.last_cte = "stage_mat_filter" %}
),
{% endif %}
{%- if source_model | length > 1 %}

row_rank_union AS (
    SELECT ru.*,
           ROW_NUMBER() OVER(
               PARTITION BY {{ dbtvault_scalefree.prefix([src_pk], 'ru') }}
               ORDER BY {{ dbtvault_scalefree.prefix([src_ldts], 'ru') }}, {{ dbtvault_scalefree.prefix([src_rsrc], 'ru') }} ASC
           ) AS row_rank_number
    FROM {{ ns.last_cte }} AS ru
    QUALIFY row_rank_number = 1
    {%- set ns.last_cte = "row_rank_union" %}
),
{% endif %}
records_to_insert AS (
    SELECT {{ dbtvault_scalefree.prefix(source_cols, 'a', alias_target='target') }}
    FROM {{ ns.last_cte }} AS a
    {%- if dbtvault_scalefree.is_any_incremental() %}
    LEFT JOIN {{ this }} AS d
    ON {{ dbtvault_scalefree.multikey(src_pk, prefix=['a','d'], condition='=') }}
    {%- endif %}
)

SELECT * FROM records_to_insert

{%- endmacro -%}


{%- macro test_link(link_hashkey, foreign_hashkeys, src_ldts='ldts', src_rsrc='rsrc', source_model) -%}

{%- if not (foreign_hashkeys is iterable and foreign_hashkeys is not string) -%}
    
    {%- if execute -%}
        {{ exceptions.raise_compiler_error("Only one foreign key provieded for this link. At least two required.") }}
    {%- endif %}

{%- endif -%}

{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}


{{ prepend_generated_by() }}

{%- set source_relation = ref(source_model) -%}

WITH

source_data AS (

    SELECT
        {{ link_hashkey }},

        {% for foreign_hashkey in foreign_hashkeys -%}
            {{ foreign_hashkey }},
        {% endfor -%}

        {{ src_ldts }},
        {{ src_rsrc }}
    FROM {{ source_relation }}

    {# Reducing the amount of data to only include ldts newer than the existing max ldts in incremental loads #}
    {%- if is_incremental() -%}
    WHERE {{ src_ldts }} > (SELECT MAX({{ src_ldts }}) 
                            FROM {{ this }}
                            WHERE {{ src_ldts }} != {{ dbtvault_scalefree.string_to_timestamp(timestamp_format, end_of_all_times) }} )
    {%- endif -%}

    {# Deudplicate the data and only get the earliest entry for each link hashkey. #}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ link_hashkey }} ORDER BY {{ src_ldts }}) = 1

    {%- set last_cte = 'source_data' -%}
)

{%- if is_incremental() -%},
distinct_target_hashkeys AS (

    SELECT DISTINCT 
    {{ link_hashkey }}
    FROM {{ this }}

),

delta AS (

    SELECT
        {{ link_hashkey }},

        {% for foreign_hashkey in foreign_hashkeys -%}
            {{ foreign_hashkey }},
        {% endfor -%}

        {{ src_ldts }},
        {{ src_rsrc }}
    FROM {{ last_cte }}
    WHERE {{ link_hashkey }} NOT IN distinct_target_hashkeys

    {%- set last_cte = 'delta' -%}
)

SELECT * FROM {{ last_cte }}

{%- endmacro -%}


