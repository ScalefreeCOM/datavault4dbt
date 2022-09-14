{%- macro exasol__eff_sat_link_v1(eff_sat_link_v0, link_hashkey, src_ldts, src_rsrc, eff_from_alias, eff_to_alias, add_is_current_flag) -%}

{%- set source_cols = dbtvault_scalefree.expand_column_list(columns=[link_hashkey, src_rsrc, src_ldts, 'is_active']) -%}
{%- set final_cols = dbtvault_scalefree.expand_column_list(columns=[link_hashkey, src_rsrc, eff_from_alias, eff_to_alias]) -%}

{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', 'YYYY-MM-DDTHH-MI-SS') -%}
{%- set is_current_col_alias = var('dbtvault_scalefree.is_current_col_alias', 'IS_CURRENT') -%}

{%- set hash = var('dbtvault_scalefree.hash', 'MD5') -%}
{%- set hash_alg, unknown_key, error_key = dbtvault_scalefree.hash_default_values(hash_function=hash) -%}

{%- set source_relation = ref(eff_sat_link_v0) -%}

{{ dbtvault_scalefree.prepend_generated_by() }}

WITH

source_data AS (

    SELECT
        {{ dbtvault_scalefree.prefix(source_cols, 'sat_v0') }}
    FROM {{ source_relation }} AS sat_v0

),

eff_ranges AS (

    SELECT
        {{ link_hashkey }},
        {{ src_rsrc }},
        is_active,
        {{ src_ldts }} AS {{ eff_from_alias }},
        COALESCE(LAG(ADD_SECONDS({{ src_ldts }}, -0.001)) OVER (PARTITION BY {{ link_hashkey }} ORDER BY {{ src_ldts }} DESC), {{ dbtvault_scalefree.string_to_timestamp( timestamp_format , end_of_all_times) }}) as {{ eff_to_alias }}
    FROM source_data

),

records_to_select AS (

    SELECT
        {{ dbtvault_scalefree.print_list(final_cols) }}
        {%- if add_is_current_flag %},
            CASE WHEN {{ eff_to_alias }} = {{ dbtvault_scalefree.string_to_timestamp(timestamp_format, end_of_all_times) }}
            THEN TRUE
            ELSE FALSE
            END AS {{ is_current_col_alias }}
        {% endif %}
    FROM eff_ranges
    WHERE is_active = true

)

SELECT * FROM records_to_select

{%- endmacro -%}
