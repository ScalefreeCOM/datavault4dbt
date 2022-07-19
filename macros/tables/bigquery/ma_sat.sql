/*  
Macro to create Multi-Active Satellites.
*/
{%- macro ma_sat(src_pk, src_hashdiff, src_payload, src_eff, src_ldts, src_rsrc, source_model) -%}

{{- adapter.dispatch('ma_sat', 'dbtvault_scalefree')((src_pk=src_pk,
                                                      src_hashdiff=src_hashdiff,
                                                      src_payload=src_payload,
                                                      src_eff=src_eff,
                                                      src_ldts=src_ldts,
                                                      src_rsrc=src_rsrc,
                                                      source_model=source_model)) -}}

{%- endmacro -%}                                                      


{%- macro default__ma_sat(src_pk, src_hashdiff, src_payload, src_eff, src_ldts, src_rsrc, source_model) -%}
 
{{- dbtvault.check_required_parameters(src_pk=src_pk, src_hashdiff=src_hashdiff, src_payload=src_payload,
                                       src_ldts=src_ldts, src_rsrc=src_rsrc,
                                       source_model=source_model) -}}
 
{%- SET source_cols = dbtvault.expand_column_list(COLUMNS=[src_pk, src_payload, src_eff, src_ldts, src_rsrc]) -%}
{%- SET latest_cols = dbtvault.expand_column_list(COLUMNS=[src_pk, src_hashdiff, src_ldts]) -%}
{%- SET all_cols    = dbtvault.expand_column_list(COLUMNS=[src_pk, src_hashdiff, src_payload, src_eff, src_ldts, src_rsrc]) -%}
 
/*  Get data from stage.
    Create sequence number as identifier and PK extension.
    Create new hashdiff based on multiple rows per pk/ldts. 
*/
WITH SOURCE AS (
 
    SELECT DISTINCT {{ dbtvault.prefix(source_cols, 'a') }}
         , DENSE_RANK() OVER (PARTITION BY {{ dbtvault.prefix([src_pk], 'a') }}, {{ dbtvault.prefix([src_ldts], 'a') }} ORDER BY CAST({{ dbtvault.prefix([src_hashdiff], 'a') }} AS STRING)) AS ma_seq
         , CAST(MD5(LISTAGG(DISTINCT CAST({{ dbtvault.prefix([src_hashdiff], 'a') }} AS VARCHAR), '||') WITHIN GROUP (ORDER BY CAST({{ dbtvault.prefix([src_hashdiff], 'a') }} AS STRING)) OVER (PARTITION BY {{ dbtvault.prefix([src_pk], 'a') }}, {{ dbtvault.prefix([src_ldts], 'a') }})) AS BINARY(16)) AS {{ src_hashdiff }}
    FROM {{ REF(source_model) }} AS a
    WHERE {{ dbtvault.prefix([src_pk], 'a') }} IS NOT NULL
)
 
/*  Create a rank to later identify the update order.
    Deduplication based on the new multi-active hashdiff.
*/
, source_data AS (
 
    SELECT {{ dbtvault.prefix(all_cols, 'a') }}
         , a.ma_seq
    {%- IF is_incremental() %}
         , DENSE_RANK() OVER (PARTITION BY {{ dbtvault.prefix([src_pk], 'a') }} ORDER BY {{ dbtvault.prefix([src_ldts], 'a') }}) AS rnk
    {%- endif %}
    FROM SOURCE AS a
    QUALIFY CASE WHEN {{ dbtvault.prefix([src_hashdiff], 'a') }} = LAG({{ dbtvault.prefix([src_hashdiff], 'a') }}) OVER (PARTITION BY {{ dbtvault.prefix([src_pk], 'a') }}, a.ma_seq ORDER BY {{ dbtvault.prefix([src_ldts], 'a') }}) THEN FALSE
                ELSE TRUE
            END
)
 
{%- IF is_incremental() %}
/*  Get latest records from Satellite. */
, latest_records AS (
 
    SELECT {{ dbtvault.prefix(latest_cols, 'current_records') }}
    FROM {{ this }} AS current_records
    INNER JOIN (
        SELECT DISTINCT {{ dbtvault.prefix([src_pk], 'source_data') }}
        FROM source_data
    ) AS source_records
    ON {{ dbtvault.prefix([src_pk], 'current_records') }} = {{ dbtvault.prefix([src_pk], 'source_records') }}
    QUALIFY RANK() OVER (PARTITION BY {{ dbtvault.prefix([src_pk], 'current_records') }} ORDER BY {{ dbtvault.prefix([src_ldts], 'current_records') }} DESC) = 1
)
{%- endif %}
 
/*  Select new records, that differ in ma-hashdiff or didn't came first (rnk!=1). */
SELECT DISTINCT {{ dbtvault.alias_all(all_cols, 'stage') }}
     , stage.ma_seq
FROM source_data AS stage
{%- IF is_incremental() %}
LEFT JOIN latest_records
ON {{ dbtvault.prefix([src_pk], 'latest_records') }} = {{ dbtvault.prefix([src_pk], 'stage') }}
WHERE {{ dbtvault.prefix([src_ldts], 'stage') }} > {{ dbtvault.prefix([src_ldts], 'latest_records') }}
    AND NOT EXISTS (SELECT 1
                    FROM latest_records
                    WHERE {{ dbtvault.prefix([src_pk], 'latest_records') }} = {{ dbtvault.prefix([src_pk], 'stage') }}
                    AND {{ dbtvault.prefix([src_hashdiff], 'latest_records') }} = {{ dbtvault.prefix([src_hashdiff], 'stage') }}
                    AND stage.rnk = 1)
    OR latest_records.{{ src_hashdiff }} IS NULL
{%- endif %}
 
{%- endmacro -%}