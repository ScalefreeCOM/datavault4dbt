{%- macro default__eff_sat_link_v0(link_hashkey, driving_key, secondary_fks, src_ldts, src_rsrc, source_model) -%}

{{- dbtvault.check_required_parameters(link_hashkey=link_hashkey, driving_key=driving_key, secondary_fks=secondary_fks,
                                       src_ldts=src_ldts, src_rsrc=src_rsrc,
                                       source_model=source_model) -}}

{%- set source_cols = dbtvault.expand_column_list(columns=[link_hashkey, driving_key, secondary_fks, src_rsrc, src_ldts]) -%}
{%- set union_cols = dbtvault.expand_column_list(columns=[link_hashkey, driving_key, secondary_fks, src_rsrc]) -%}
{%- set final_cols = dbtvault.expand_column_list(columns=[link_hashkey, driving_key, secondary_fks, src_ldts, src_rsrc]) -%}


WITH

{#
    Get all records from staging layer where driving key and secondary foreign keys are not null.
    Deduplicate over HK+Driving Key uneuqls the previous (regarding src_ldts) combination.
#}
stage AS (
    SELECT
        {{ dbtvault.prefix(source_cols, 'source') }}
    FROM {{ ref(source_model) }} AS source
    WHERE {{ dbtvault.multikey(driving_key, prefix='source', condition='IS NOT NULL') }}
    AND {{ dbtvault.multikey(secondary_fks, prefix='source', condition='IS NOT NULL') }}
    QUALIFY CASE WHEN {{ dbtvault.prefix([link_hashkey], 'source') }} = LAG({{ dbtvault.prefix([link_hashkey], 'source') }}) OVER (PARTITION BY {{ dbtvault.prefix([driving_key], 'source') }} ORDER BY {{ dbtvault.prefix([src_ldts], 'source') }}) THEN FALSE
                 ELSE TRUE
            END
),

{%- if is_incremental() %}

{#
    Get the latest record for each driving key, already existing in eff_sat and included in incoming batch. Only applied if incremental.
#}

latest_record AS (
    SELECT
        {{ dbtvault.prefix(source_cols, 'current_records') }}
    FROM {{ this }} AS current_records
    INNER JOIN (
        SELECT DISTINCT
            {{ dbtvault.prefix([driving_key], 'stage') }}
        FROM stage
    ) AS source_records
        ON {{ dbtvault.multikey(driving_key, prefix=['current_records', 'source_records'], condition='=') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ dbtvault.prefix([driving_key], 'current_records') }} ORDER BY {{ dbtvault.prefix([src_ldts], 'current_records') }} DESC) = 1
),
{%- endif %}

{#
    Select only incoming records from the stage, that are newer than the latest record in the eff_sat, or when it does not exist yet.
    Creates the src_ldts_lead for intermediate changes, and a rank column over the driving key, order by the ldts.
#}
stage_new AS (
    SELECT
        {{ dbtvault.prefix(source_cols, 'stage') }},
        LEAD({{ dbtvault.prefix([src_ldts], 'stage') }}) OVER (PARTITION BY {{ dbtvault.prefix([driving_key], 'stage') }} ORDER BY {{ dbtvault.prefix([src_ldts], 'stage') }}) AS src_ldts_lead,
        ROW_NUMBER() OVER (PARTITION BY {{ dbtvault.prefix([driving_key], 'stage') }} ORDER BY {{ dbtvault.prefix([src_ldts], 'stage') }}) as stage_rank,
    FROM stage
    {%- if is_incremental() %}
    LEFT JOIN latest_record
        ON {{ dbtvault.multikey(driving_key, prefix=['stage', 'latest_record'], condition='=') }}
    WHERE {{ dbtvault.prefix([src_ldts], 'stage') }} > {{ dbtvault.prefix([src_ldts], 'latest_record') }}
        OR {{ dbtvault.prefix([src_ldts], 'latest_record') }} IS NULL
    {%- endif %}
),

{%- if is_incremental() -%}

{#
    Disable all latest records in the eff_sat, when there is a new relationship for that driving key in the stage.
#}

deactivated_existing AS (

    SELECT
        {{ dbtvault.prefix(union_cols, 'latest_record') }},
        {{ dbtvault.prefix([src_ldts], 'stage_new') }} AS {{ src_ldts }},
        FALSE AS is_active
    FROM latest_record
    LEFT JOIN stage_new
        ON {{ dbtvault.multikey(driving_key, prefix=['latest_record', 'stage_new'], condition='=') }}
    WHERE {{ dbtvault.prefix([link_hashkey], 'latest_record') }} != {{ dbtvault.prefix([link_hashkey], 'stage_new') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ dbtvault.prefix([driving_key], 'stage_new') }} ORDER BY {{ dbtvault.prefix([src_ldts], 'stage_new') }}) = 1

),

{%- endif %}

{#
    Activate all rows that have a different relationship for an existing driving key OR where the driving key is not yet existing in the eff_sat
    OR is not the first new one in the incoming data (due to intermediate changes).
#}

activated_new_records AS (

    SELECT
        {{ dbtvault.prefix(union_cols, 'stage_new') }},
        {{ dbtvault.prefix([src_ldts], 'stage_new') }} AS {{ src_ldts }},
        TRUE AS is_active
    FROM stage_new
    {%- if is_incremental() %}
    LEFT JOIN latest_record
        ON {{ dbtvault.multikey(driving_key, prefix=['stage_new', 'latest_record'], condition='=') }}
    WHERE {{ dbtvault.prefix([link_hashkey], 'stage_new') }} != {{ dbtvault.prefix([link_hashkey], 'latest_record') }}
        OR {{ dbtvault.prefix([src_ldts], 'latest_record') }} IS NULL
        OR stage_new.stage_rank != 1
    {%- endif %}

),

{#
    Deactivate all intermediate changes that are not the latest one.
#}

deactivated_intermediates AS (

    SELECT
        {{ dbtvault.prefix(union_cols, 'stage_new') }},
        stage_new.src_ldts_lead AS src_ldts,
        FALSE AS is_active
    FROM stage_new
    {%- if is_incremental() %}
    LEFT JOIN latest_record
        ON {{ dbtvault.multikey(driving_key, prefix=['stage_new', 'latest_record'], condition='=') }}
    WHERE {{ dbtvault.prefix([link_hashkey], 'stage_new') }} != {{ dbtvault.prefix([link_hashkey], 'latest_record') }}
        OR {{ dbtvault.prefix([src_ldts], 'latest_record') }} IS NULL
        OR stage_new.stage_rank != 1
    {%- endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ dbtvault.prefix([driving_key], 'stage_new') }} ORDER BY {{ dbtvault.prefix([src_ldts], 'stage_new') }} DESC) != 1

),

{#
    Unionize all three cases for final insertion.
#}

final_columns_to_select AS (

    SELECT * FROM activated_new_records

    UNION ALL

    SELECT * FROM deactivated_intermediates

    {% if is_incremental() -%}
    UNION ALL

    SELECT * FROM deactivated_existing
    {%- endif %}
)

SELECT * FROM final_columns_to_select

{% endmacro %}
