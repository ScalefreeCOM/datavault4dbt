{%- macro synapse__eff_sat_v0(source_model, tracked_hashkey, src_ldts, src_rsrc, is_active_alias, source_is_single_batch, disable_hwm) -%}

{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- set ns = namespace(last_cte= "") -%}

{%- set source_relation = ref(source_model) -%}

{%- set tracked_hashkey = datavault4dbt.escape_column_names(tracked_hashkey) -%}
{%- set is_active_alias = datavault4dbt.escape_column_names(is_active_alias) -%}
{%- set src_ldts = datavault4dbt.escape_column_names(src_ldts) -%}
{%- set src_rsrc = datavault4dbt.escape_column_names(src_rsrc) -%}

{{ log('columns to select: '~final_columns_to_select, false) }}

{{ datavault4dbt.prepend_generated_by() }}

WITH 

{#
    In all cases, the source model is selected, and optionally a HWM is applied. 
#}
source_data AS (

    SELECT
        {{ tracked_hashkey }},
        {{ src_ldts }}
    FROM {{ source_relation }} src
    WHERE {{ src_ldts }} NOT IN ('{{ datavault4dbt.beginning_of_all_times() }}', '{{ datavault4dbt.end_of_all_times() }}')
    {%- if is_incremental() and not disable_hwm %}
    AND src.{{ src_ldts }} > (
        SELECT
            MAX({{ src_ldts }})
        FROM {{ this }}
        WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
    )
    {%- endif %}
),

{#
    In all incremental cases, the current status for each hashkey is selected from the existing Effectivity Satellite.
#}
{%- if is_incremental() %}
current_status_prep AS (

    SELECT
        {{ tracked_hashkey }},
        {{ is_active_alias}},
        ROW_NUMBER() OVER (PARTITION BY {{ tracked_hashkey }} ORDER BY {{ src_ldts }} DESC) as rn
    FROM {{ this }}

),

current_status AS (

    SELECT
        {{ tracked_hashkey }},
        {{ is_active_alias }}
    FROM current_status_prep
    WHERE rn = 1 

),
{% endif %}

{#
    This block is for multi-batch processing. 
#}
{% if not source_is_single_batch %}

    {#
        List of all Hashkeys with their date of first appearance in the source model.
    #}
    hashkeys AS (

        SELECT 
            {{ tracked_hashkey }},
            MIN({{ src_ldts }}) as first_appearance
        FROM source_data
        GROUP BY {{ tracked_hashkey }}

    ),

    {#
        Distinct list of load dates in the multi-batch source.
    #}
    load_dates AS (

        SELECT Distinct
            {{ src_ldts }}
        FROM source_data
        
    ),

    {#
        All combinations of hashkeys and loaddates, for loaddates after the first appearance of a hashkey.
    #}
    history AS (

        SELECT 
            hk.{{ tracked_hashkey }},
            ld.{{ src_ldts }}
        FROM hashkeys hk
        CROSS JOIN load_dates ld
        WHERE ld.{{ src_ldts }} >= hk.first_appearance

    ),

    {#
        All theoretical combinations are checked against the actual occurences of hashkeys in each batch / loaddate.
        If a Hashkey is part of a load/batch, is_active_alias is set to 1, because the hashkey was active in that load/batch.
        If a Hashkey is not part of a load/batch, is_active_alias is set to 0, because the hashkey was not active in that load/batch.
    #}
    is_active AS (

        SELECT
            h.{{ tracked_hashkey }},
            h.{{ src_ldts }},
            CASE 
                WHEN src.{{ tracked_hashkey }} IS NULL THEN 0
                ELSE 1 
            END as {{ is_active_alias }}
        FROM history h
        LEFT JOIN source_data src
            ON src.{{ tracked_hashkey }} = h.{{ tracked_hashkey }}
            AND src.{{ src_ldts }} = h.{{ src_ldts }}

    ),

    {#
        The rows are deduplicated on the is_active_alias, to only include status changes. 
        Additionally, a ROW_NUMBER() is calculated in incremental runs, to use it in the next step for comparison against the current status.
    #}
    deduplicated_incoming AS (

        SELECT
            is_active.{{ tracked_hashkey }},
            is_active.{{ src_ldts }},
            is_active.{{ is_active_alias }}

            {% if is_incremental() -%}
            , ROW_NUMBER() OVER(PARTITION BY is_active.{{ tracked_hashkey }} ORDER BY is_active.{{ src_ldts }}) as rn
            {%- endif %}        

        FROM is_active
        QUALIFY 
            CASE 
                WHEN is_active.{{ is_active_alias }} = LAG(is_active.{{ is_active_alias }}) OVER (PARTITION BY {{ tracked_hashkey }} ORDER BY {{ src_ldts }}) THEN FALSE
                ELSE TRUE
            END

    ),

    {% set ns.last_cte = 'deduplicated_incoming' %}

{#
    This block is for single-batch processing
#}
{% else %}

    {#
        In initial loads of single-batch eff sats, every hashkey of the source is set to active.
    #}
    new_hashkeys AS (

        SELECT DISTINCT
            src.{{ tracked_hashkey }},
            src.{{ src_ldts }},
            1 as {{ is_active_alias }}
        FROM source_data src

        {#
            For incremental runs of single-batch eff sats, only hashkeys that are not active right now are set to active. 
            This automatically includes totally new hashkeys, or hashkeys that are currently set to inactive.
        #}
        {% if is_incremental() %}
            LEFT JOIN current_status cs
                ON src.{{ tracked_hashkey }} = cs.{{ tracked_hashkey }}
                AND cs.{{ is_active_alias }} = 1
            WHERE cs.{{ tracked_hashkey }} IS NULL
        {% endif %}

    ),

    {% set ns.last_cte = 'new_hashkeys' %}

{% endif %}

{#
    In all incremental runs, the source needs to be scanned for all currently active hashkeys. 
    If they are no longer present, they will be deactived. 
#}
{%- if is_incremental() %}

    {%- if not source_is_single_batch %}
        disappeared_hashkeys AS (

            SELECT DISTINCT 
                cs.{{ tracked_hashkey }},
                ldts.min_ldts as {{ src_ldts }},
                0 as {{ is_active_alias }}
            FROM current_status cs
            LEFT JOIN (
                SELECT 
                    MIN({{ src_ldts }}) as min_ldts
                FROM deduplicated_incoming) ldts
                ON 1 = 1
            LEFT JOIN deduplicated_incoming src
                ON src.{{ tracked_hashkey }} = cs.{{ tracked_hashkey }}
                AND  src.{{ src_ldts }} = ldts.min_ldts
            WHERE
                cs.{{ is_active_alias }} = 1
                AND src.{{ tracked_hashkey }} IS NULL
                AND ldts.min_ldts IS NOT NULL

        ),
    {% else %}
        disappeared_hashkeys AS (

            SELECT DISTINCT 
                cs.{{ tracked_hashkey }},
                ldts.min_ldts as {{ src_ldts }},
                0 as {{ is_active_alias }}
            FROM current_status cs
            LEFT JOIN (
                SELECT 
                    MIN({{ src_ldts }}) as min_ldts
                FROM source_data) ldts
                ON 1 = 1
            WHERE NOT EXISTS (
                SELECT 
                    1 
                FROM source_data src
                WHERE src.{{ tracked_hashkey }} = cs.{{ tracked_hashkey }}
            )
            AND cs.{{ is_active_alias }} = 1
            AND ldts.min_ldts IS NOT NULL

        ),
    {% endif %}
{%- endif %}

records_to_insert AS (

    {#
        This first part of the UNION includes:
            - for single-batch loads: Only is_active_alias = 1, deactivations are handled later
            - for multi-batch loads: Ativation and deactivation inside the multiple loads
    #}
    SELECT
        di.{{ tracked_hashkey }},
        di.{{ src_ldts }},
        di.{{ is_active_alias }}
    FROM {{ ns.last_cte }} di


    {%- if is_incremental() %}

        {#
            For incremental multi-batch loads, the earliest to-be inserted status is compared to the current status. 
            It will only be inserted if the status changed. We use the ROW_NUMBER() 
        #} 
        {%- if not source_is_single_batch %}
            WHERE NOT EXISTS (
                SELECT 1
                FROM current_status
                WHERE {{ datavault4dbt.multikey(tracked_hashkey, prefix=['current_status', 'di'], condition='=') }}
                    AND {{ datavault4dbt.multikey(is_active_alias, prefix=['current_status', 'di'], condition='=') }}
                    AND di.{{ src_ldts }} = (SELECT MIN({{ src_ldts }}) FROM deduplicated_incoming)
                )
            AND di.{{ src_ldts }} > (SELECT MAX({{ src_ldts }}) FROM {{ this }})
        {% endif %}

    {#
        For all incremental loads, the disappeared hashkeys are UNIONed.
    #}
    UNION

    SELECT
        {{ tracked_hashkey }},
        {{ src_ldts }},
        {{ is_active_alias }}
    FROM disappeared_hashkeys

    {%- endif %}    

)

SELECT * 
FROM records_to_insert ri

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.{{ tracked_hashkey }} = ri.{{ tracked_hashkey }}
        AND t.{{ src_ldts }} = ri.{{ src_ldts }}
)
{% endif %}

{%- endmacro -%}