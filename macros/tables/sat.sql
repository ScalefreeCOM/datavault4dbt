{%- macro sat(src_pk, src_hashdiff, src_payload, src_eff, src_ldts, src_source, source_model) -%}

{%- set source_cols = dbtvault.expand_column_list(columns=[src_pk, src_hashdiff, src_ldts, src_source, src_payload, src_eff]) -%}
{%- set rank_cols = dbtvault.expand_column_list(columns=[src_pk, src_hashdiff, src_ldts]) -%}
{%- set pk_cols = dbtvault.expand_column_list(columns=[src_pk]) -%}

WITH
{%- if is_incremental() %}
target AS (
    SELECT
        {{ src_pk|lower() }},
        {{ src_hashdiff['source_column'] }}
    FROM {{ this }}
    WHERE {{ src_pk|lower() }} IN (
        SELECT {{ src_pk|lower() }} 
        FROM {{ ref(source_model) }}
        WHERE {{ src_ldts }} > (
            SELECT 
                MAX({{ src_ldts }}) FROM {{ this }}
            WHERE {{ src_ldts }} != '9999-12-31 23:59:59 UTC'
            )
        )
    QUALIFY ROW_NUMBER() OVER(PARTITION BY {{ src_pk|lower() }} ORDER BY {{ src_ldts }} DESC) = 1
    ),
{% endif %}

base AS (

    SELECT
        {{ src_pk|lower() }},
        {{ src_hashdiff['source_column'] }},
        {{ src_ldts }},
        {{ src_source }},
    {%- if is_incremental() -%}
        ROW_NUMBER() OVER(PARTITION BY {{ src_pk|lower() }} ORDER BY {{ src_ldts }}) as rn,
    {%- endif -%}
    {%- for col in src_payload %}
        {{ col }}
        {{- ',' if not loop.last -}}
    {%- endfor %}
    FROM {{ ref(source_model) }}
    {%- if is_incremental() %}
    WHERE {{ src_ldts }} > (SELECT MAX({{ src_ldts }}) FROM {{ this }})
    {%- endif -%}
QUALIFY CASE
            WHEN {{ src_hashdiff['source_column'] }} = LAG({{ src_hashdiff['source_column'] }}) OVER(PARTITION BY {{ src_pk|lower() }} ORDER BY {{ src_ldts }}) THEN FALSE
            ELSE TRUE
        END

)

SELECT 
    {{ src_pk|lower() }},
    {{ src_hashdiff['source_column'] }},
    {{ src_ldts }},
    {{ src_source }},
    {%- for col in src_payload %}
        {{ col }}
        {{- ',' if not loop.last -}}
    {%- endfor %}
FROM base
WHERE {{ src_pk|lower() }} != 'ffffffffffffffffffffffffffffffff'
{%- if is_incremental() %}
    AND NOT EXISTS (SELECT 1 
                    FROM target
                    WHERE target.{{ src_pk|lower() }} = base.{{ src_pk|lower() }}
                        AND target.{{ src_hashdiff['source_column'] }} = base.{{ src_hashdiff['source_column'] }}
                        AND base.rn = 1)
{%- endif -%}                        
 
{%- endmacro -%}