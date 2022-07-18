{%- macro sat_v0(src_pk, src_hashdiff, src_payload, src_ldts, src_rsrc, source_model) -%}

    {{ adapter.dispatch('sat_v0', 'dbtvault')(src_pk=src_pk, 
                                         src_hashdiff=src_hashdiff,
                                         src_payload=src_payload,
                                         src_ldts=src_ldts,
                                         src_rsrc=src_rsrc,
                                         source_model=source_model) }}

{%- endmacro -%}                                         

{%- macro default__sat_v0(src_pk, src_hashdiff, src_payload, src_ldts, src_rsrc, source_model) -%}

{%- set hash = var('hash', 'MD5') -%}
{%- if hash == 'MD5' -%}
    {%- set error_key = 'ffffffffffffffffffffffffffffffff' -%}
{%- elif hash == 'SHA' or hash == 'SHA1' -%}
    {%- set error_key = 'ffffffffffffffffffffffffffffffffffffffff' -%}
{%- elif hash == 'SHA2' or hash == 'SHA256' -%}
    {%- set error_key = 'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' -%}
{%- endif -%}

{%- set beginning_of_all_times = var('beginning_of_all_times', '0001-01-01T00-00-01') -%}
{%- set end_of_all_times = var('end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}

{{ prepend_generated_by() }}



WITH
{%- if is_incremental() %}
target AS (
    SELECT
        {{ src_pk|lower }},
        {{ src_hashdiff }}
    FROM {{ this }}
    WHERE {{ src_pk|lower }} IN (
        SELECT {{ src_pk|lower }} 
        FROM {{ ref(source_model) }}
        WHERE {{ src_ldts }} > (
            SELECT 
                MAX({{ src_ldts }}) FROM {{ this }}
            WHERE {{ src_ldts }} != PARSE_TIMESTAMP('{{ timestamp_format }}', '{{ end_of_all_times }}')
            )
        )
    QUALIFY ROW_NUMBER() OVER(PARTITION BY {{ src_pk|lower }} ORDER BY {{ src_ldts }} DESC) = 1
    ),
{% endif %}

base AS (

    SELECT
        {{ src_pk|lower }},
        {{ src_ldts }},
        {{ src_hashdiff }},
        {{ src_rsrc }},
    {% if is_incremental() %}
        ROW_NUMBER() OVER(PARTITION BY {{ src_pk|lower }} ORDER BY {{ src_ldts }}) as rn,
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
            WHEN {{ src_hashdiff }} = LAG({{ src_hashdiff }}) OVER(PARTITION BY {{ src_pk|lower }} ORDER BY {{ src_ldts }}) THEN FALSE
            ELSE TRUE
        END

)

SELECT 
    {{ src_pk|lower }},
    {{ src_ldts }},
    {{ src_hashdiff }},
    {{ src_rsrc }},
    {%- for col in src_payload %}
    {{ col }}
    {{- ',' if not loop.last -}}
    {%- endfor %}
FROM base
WHERE {{ src_pk|lower }} != '{{ error_key }}'
{%- if is_incremental() %}
{# Check if each record is already in the target CTE, if yes, then no delta, if no, then delta #}
    AND NOT EXISTS (SELECT 1 
                    FROM target
                    WHERE target.{{ src_pk|lower }} = base.{{ src_pk|lower }}
                        AND target.{{ src_hashdiff }} = base.{{ src_hashdiff }}
                        AND base.rn = 1)
{%- endif -%}                        
 
{%- endmacro -%}