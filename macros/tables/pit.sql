{%- macro pit(pit_type, src_hub, src_pk, sat_names, src_ldts, use_logarithmic_snap, snapshot_table) -%}

    {{ return(adapter.dispatch('pit')(pit_type
                                    , src_hub
                                    , src_pk
                                    , sat_names
                                    , src_ldts
                                    , use_logarithmic_snap
                                    , snapshot_table)) }}

{%- endmacro -%}

{%- macro default__pit(pit_type, src_hub, src_pk, sat_names, src_ldts, use_logarithmic_snap, snapshot_table) -%}

{%- set ghost_pk = '00000000000000000000000000000000' -%}
{%- set ghost_date = '0001-01-01 00:00:00.000' %}

{%- set snapshot_relation = ref(snapshot_table) -%}

{%- set unique_hash_alias = src_pk | replace('_h', '_d') -%}

SELECT
    '{{ pit_type }}' as type,
    concat('https://scalefree-edw-docs.s3.eu-west-1.amazonaws.com/dbt-docs/index.html#!/model/model.bigquery_edw.', '{{ this.table }}') as rsrc,
    {{ hash(columns=[dbtvault.prefix([src_pk], 'h'), dbtvault.prefix(['sdts'], 'snap')],
                alias=unique_hash_alias,
                is_hashdiff=false)   }} ,
    h.{{ src_pk }},
    snap.sdts,
    {% for satellite in sat_names %}
        COALESCE({{ satellite }}.{{ src_pk }}, CAST('{{ ghost_pk }}' AS STRING)) AS hk_{{ satellite }},
        COALESCE({{ satellite }}.{{ src_ldts }}, CAST('{{ ghost_date }}' AS {{ dbtvault.type_timestamp() }})) AS ldts_{{ satellite }}
        {{- "," if not loop.last }}
    {% endfor %}

FROM 
        {{ ref(src_hub) }} h
    FULL OUTER JOIN 
        {{ snapshot_relation }} snap
        ON 
        {% if use_logarithmic_snap %}
            snap.is_logarithmic_snap = true
        {% else %}
            1=1
        {%- endif %}
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
WHERE snap.is_active
{%- if is_incremental() %}
    AND bp.{{ src_pk }} IS NULL
    AND bp.sdts IS NULL
    AND bp.type IS NULL
{% endif -%}    

{%- endmacro -%}