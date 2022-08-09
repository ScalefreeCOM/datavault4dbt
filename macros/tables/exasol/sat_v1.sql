
{%- macro exasol__sat_v1(source_sat, src_hk, src_hd, src_ldts, ledts_alias) -%}

    {%- set all_columns = adapter.get_columns_in_relation(ref(source_sat)) -%}
    {%- set exclude = [src_hk | upper , src_hd | upper, src_ldts] -%}

    {%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
    {%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', 'YYYY-mm-ddTHH-MI-SS') -%}

    {%- set hash = var('dbtvault_scalefree.hash', 'MD5') -%}
    {%- set hash_alg, unknown_key, error_key = dbtvault_scalefree.hash_default_values(hash_function=hash) -%}

    {{ dbtvault_scalefree.prepend_generated_by() }}

    SELECT
        {{ src_hk }},
        {{ src_ldts }},
        COALESCE(LEAD(ADD_SECONDS( {{ src_ldts }}, -0.001)) OVER (PARTITION BY {{ src_hk }} ORDER BY {{ src_ldts }}),
                                    {{ dbtvault_scalefree.string_to_timestamp(timestamp_format, end_of_all_times) }}) as {{ ledts_alias }},
        {{ src_hd }},
        {%- for column in all_columns -%}
            {%- if column.name not in exclude -%}
                {{ column.name }}
                {{ "," if not loop.last }}
            {%- endif -%}
        {%- endfor -%}
    FROM {{ ref(source_sat) }}
    WHERE {{ src_hd }} != '{{ error_key }}'

{%- endmacro -%}
