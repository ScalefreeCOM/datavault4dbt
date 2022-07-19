{%- macro hub(hashkey, business_key, src_ldts, src_rsrc, source_model) -%}

    {{ return(adapter.dispatch('hub', 'dbtvault_scalefree')(hashkey=hashkey,
                                                  business_key=business_key,
                                                  src_ldts=src_ldts,
                                                  src_rsrc=src_rsrc,
                                                  source_model=source_model)) }}

{%- endmacro -%}                                                  

{%- macro default__hub(hashkey, business_key, src_ldts, src_rsrc, source_model) -%}

{%- set beginning_of_all_times = var('beginning_of_all_times', '0001-01-01T00-00-01') -%}
{%- set end_of_all_times = var('end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}

{%- if not (business_key is iterable and business_key is not string) -%}
{%- set business_keys = [business_key] -%}
{%- else -%}
{%- set business_keys = business_key -%}
{%- endif -%}

{{ prepend_generated_by() }}

WITH

source_data AS (

    SELECT 
        {{ hashkey }},
        {% for business_key in business_keys -%}
        {{ business_key|lower() }},
        {% endfor -%}
        {{ src_ldts }},
        {{ src_rsrc }},
    FROM {{ ref(source_model) }}

    {%- set last_cte = 'source_data' -%}
)

{%- if is_incremental() %},

delta AS (

    SELECT 
        * 
    FROM {{ last_cte }}
    WHERE {{ src_ldts }} > (SELECT MAX({{ src_ldts }}) 
                            FROM {{ this }}
                            WHERE {{ src_ldts }} != PARSE_TIMESTAMP('{{ timestamp_format }}', '{{ end_of_all_times }}'))
    AND {{ get_standard_string(business_keys) }} 
        NOT IN (SELECT {{ get_standard_string(business_keys) }}
                FROM {{ this }})

    {%- set last_cte = 'delta' -%}
)

{%- endif -%}                                        

SELECT * FROM {{ last_cte }}

{%- endmacro -%}