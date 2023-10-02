{%- macro n_sat(src_pk, src_payload, src_eff, src_ldts, src_source, source_model) -%}

    {{- adapter.dispatch('n_sat', 'datavault4dbt')(src_pk=src_pk, src_payload=src_payload, 
                                            src_eff=src_eff, src_ldts=src_ldts,
                                            src_source=src_source, source_model=source_model) -}}

{%- endmacro %}

{%- macro default__n_sat(src_pk, src_payload, src_eff, src_ldts, src_source, source_model) -%}

{{- datavault4dbt.check_required_parameters(src_pk=src_pk, src_payload=src_payload,
                                       src_ldts=src_ldts, src_source=src_source,
                                       source_model=source_model) -}}

{%- set source_cols = datavault4dbt.expand_column_list(columns=[src_pk, src_ldts, src_source, src_payload, src_eff]) -%}
{%- set rank_cols = datavault4dbt.expand_column_list(columns=[src_pk, src_ldts]) -%}
{%- set pk_cols = datavault4dbt.expand_column_list(columns=[src_pk]) -%}

{%- if model.config.materialized == 'vault_insert_by_rank' %}
    {%- set source_cols_with_rank = source_cols + [config.get('rank_column')] -%}
{%- endif -%}

{{ datavault4dbt.prepend_generated_by() }}

WITH source_data AS (
    {%- if model.config.materialized == 'vault_insert_by_rank' %}
    SELECT {{ datavault4dbt.prefix(source_cols_with_rank, 'a', alias_target='source') }}
    {%- else %}
    SELECT {{ datavault4dbt.prefix(source_cols, 'a', alias_target='source') }}
    {%- endif %}
    FROM {{ ref(source_model) }} AS a
    WHERE {{ datavault4dbt.multikey(src_pk, prefix='a', condition='IS NOT NULL') }}
    {%- if model.config.materialized == 'vault_insert_by_period' %}
    AND __PERIOD_FILTER__
    {% elif model.config.materialized == 'vault_insert_by_rank' %}
    AND __RANK_FILTER__
    {% endif %}
),

{%- if datavault4dbt.is_any_incremental() %}

latest_records AS (

    SELECT {{ datavault4dbt.prefix(rank_cols, 'a', alias_target='target') }}
    FROM (
        SELECT {{ datavault4dbt.prefix(rank_cols, 'current_records', alias_target='target') }},
            RANK() OVER (
                PARTITION BY {{ datavault4dbt.prefix([src_pk], 'current_records') }}
                ORDER BY {{ datavault4dbt.prefix([src_ldts], 'current_records') }} DESC
            ) AS rank
        FROM {{ this }} AS current_records
            JOIN (
                SELECT DISTINCT {{ datavault4dbt.prefix([src_pk], 'source_data') }}
                FROM source_data
            ) AS source_records
                ON {{ datavault4dbt.multikey(src_pk, prefix=['current_records', 'source_records'], condition='=') }}
    ) AS a
    WHERE a.rank = 1
),

{%- endif %}

records_to_insert AS (
    SELECT DISTINCT {{ datavault4dbt.alias_all(source_cols, 'stage') }}
    FROM source_data AS stage
    {%- if datavault4dbt.is_any_incremental() %}
        LEFT JOIN latest_records
            ON {{ datavault4dbt.multikey(src_pk, prefix=['latest_records','stage'], condition='=') }}
            WHERE {{ datavault4dbt.prefix([src_pk], 'latest_records', alias_target='target') }} IS NULL
    {%- endif %}
)

SELECT * FROM records_to_insert

{%- endmacro -%}