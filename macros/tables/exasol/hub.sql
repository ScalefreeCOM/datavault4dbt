{%- macro exasol__hub(hashkey, business_key, src_ldts, src_rsrc, source_model) -%}

{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', 'YYYY-mm-ddTHH-MI-SS') -%}

{%- if not (business_key is iterable and business_key is not string) -%}
{%- set business_keys = [business_key] -%}
{%- else -%}
{%- set business_keys = business_key -%}
{%- endif -%}

{{ dbtvault_scalefree.prepend_generated_by() }}

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
                            WHERE {{ src_ldts }} != {{ dbtvault_scalefree.string_to_timestamp(timestamp_format, end_of_all_times) }} )
    AND {{ dbtvault_scalefree.get_standard_string(business_keys) }} 
        NOT IN (SELECT {{ dbtvault_scalefree.get_standard_string(business_keys) }}
                FROM {{ this }})

    {%- set last_cte = 'delta' -%}
)

{%- endif -%}                                        

SELECT * FROM {{ last_cte }}

{%- endmacro -%}