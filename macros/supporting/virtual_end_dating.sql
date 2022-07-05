{%- macro virtual_end_dating(source_sat, src_hk, src_hd) -%}

    {%- set all_columns = adapter.get_columns_in_relation(ref(source_sat)) -%}
    {%- set exclude = [src_hk, src_hd, "ldts"] -%}



    SELECT
        {{ src_hk }},
        ldts,
        COALESCE(LEAD(TIMESTAMP_SUB(ldts, INTERVAL 1 MICROSECOND)) OVER (PARTITION BY {{ src_hk }} ORDER BY ldts),CAST('8888-12-31 23:59:59.000000' as timestamp)) as ledts,
        {{ src_hd }},
        {%- for column in all_columns -%}
            {%- if column.name not in exclude -%}
                {{ column.name }}
                {{ "," if not loop.last }}
            {%- endif -%}
        {%- endfor -%}
    FROM {{ ref(source_sat) }}
    WHERE {{ src_hd }} != 'ffffffffffffffffffffffffffffffff'
{%- endmacro -%}
