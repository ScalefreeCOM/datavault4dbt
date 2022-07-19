{%- macro pit(pit_type, src_hub, src_pk, sat_names, src_ldts, rsrc, snapshot_relation, snapshot_trigger_column, dimension_key) -%}

    {{ return(adapter.dispatch('pit')('dbtvault_scalefree')(pit_type
                                    , src_hub
                                    , src_pk
                                    , sat_names
                                    , src_ldts
                                    , rsrc
                                    , snapshot_relation
                                    , snapshot_trigger_column
                                    , dimension_key)) }}

{%- endmacro -%}

{%- macro default__pit(pit_type, src_hub, src_pk, sat_names, src_ldts, rsrc, snapshot_relation, snapshot_trigger_column, dimension_key) -%}

{%- set hash = var('hash', 'MD5') -%}
{%- if hash == 'MD5' -%}
    {%- set zero_key = '00000000000000000000000000000000' -%}
{%- elif hash == 'SHA' or hash == 'SHA1' -%}
    {%- set zero_key = '0000000000000000000000000000000000000000' -%}
{%- elif hash == 'SHA2' or hash == 'SHA256' -%}
    {%- set zero_key = '0000000000000000000000000000000000000000000000000000000000000000' -%}
{%- endif -%}

{%- set beginning_of_all_times = var('beginning_of_all_times', '0001-01-01T00-00-01') -%}
{%- set snapshot_relation = ref(snapshot_relation) -%}

{{ prepend_generated_by() }}

SELECT
    '{{ pit_type }}' as type,
    '{{ rsrc }}' as rsrc,
    {{ hash(columns=[dbtvault_scalefree.prefix([src_pk], 'h'), dbtvault_scalefree.prefix(['sdts'], 'snap')],
                alias=dimension_key,
                is_hashdiff=false)   }} ,
    h.{{ src_pk }},
    snap.sdts,
    {% for satellite in sat_names %}
        COALESCE({{ satellite }}.{{ src_pk }}, CAST('{{ zero_key }}' AS STRING)) AS hk_{{ satellite }},
        COALESCE({{ satellite }}.{{ src_ldts }}, CAST('{{ beginning_of_all_times }}' AS {{ dbtvault.type_timestamp() }})) AS ldts_{{ satellite }}
        {{- "," if not loop.last }}
    {% endfor %}

FROM 
        {{ ref(src_hub) }} h
    FULL OUTER JOIN 
        {{ snapshot_relation }} snap
        ON snap.{{ snapshot_trigger_column }} = true
    {%- if is_incremental() %}
    LEFT JOIN 
        {{ this }} bp 
        ON
            bp.{{ src_pk }} = h.{{ src_pk }} 
            AND bp.sdts = snap.sdts 
            AND bp.type = '{{ pit_type }}'
    {% endif -%}
    {% for satellite in sat_names %}
    LEFT JOIN {{ ref(satellite) }}
        ON
            {{ satellite }}.{{ src_pk}} = h.{{ src_pk }} 
            AND snap.sdts BETWEEN {{ satellite }}.ldts AND {{ satellite }}.ledts
    {% endfor %}            
WHERE snap.{{ snapshot_trigger_column }}
{%- if is_incremental() %}
    AND bp.{{ src_pk }} IS NULL
    AND bp.sdts IS NULL
    AND bp.type IS NULL
{% endif -%}    

{%- endmacro -%}