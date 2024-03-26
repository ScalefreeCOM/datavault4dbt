{#
    This macro should be used as a post hook for each PIT table, whenever a logarithmic snapshot logic is used.
    The macro deletes all records in a PIT table, that are no longer active. Deletion is no problem here because
    no actual data is deleted, only pointers to satellite entries.

    Parameters:
        snapshot_relation::string       The name of the dbt model that creates the snapshot table / view, that has
                                        the logarithmic snapshot logic applied.

        snapshot_trigger_column::string The name of the boolean column inside the snapshot tables, that activate/deactivate
                                        single snapshots. If not set, the name defined inside the global variable
                                        'datavault4dbt.snapshot_trigger_column' is used.
                                        
        sdts::string                    The name of the snapshot date timestamp column inside the snapshot table. If not set,
                                        the name defined inside the global variable 'datavault4dbt.sdts_alias' is used.

    Example Usage:

        An example usage for applying this macro as a post hook for a PIT table would look like this inside the PIT source_models
        config block:

            "{{ config(post_hook="{{ datavault4dbt.clean_up_pit('control_snap_view') }}") }}"

#}


{%- macro clean_up_pit(snapshot_relation, snapshot_trigger_column=none, sdts=none) -%}

{%- if not datavault4dbt.is_something(sdts) -%}
    {%- set sdts = var('datavault4dbt.sdts_alias', 'sdts') -%}
{%- endif -%}
{%- if not datavault4dbt.is_something(snapshot_trigger_column) -%}
    {%- set snapshot_trigger_column = var('datavault4dbt.snapshot_trigger_column', 'is_active') -%}
{%- endif -%}

{{ return(adapter.dispatch('clean_up_pit', 'datavault4dbt')(snapshot_relation=snapshot_relation, snapshot_trigger_column=snapshot_trigger_column, sdts=sdts)) }}

{%- endmacro -%}

{%- macro default__clean_up_pit(snapshot_relation, snapshot_trigger_column, sdts) -%}

DELETE {{ this }} pit
WHERE pit.{{ sdts }} not in (SELECT {{ sdts }} FROM {{ ref(snapshot_relation) }} snap WHERE {{ snapshot_trigger_column }}=TRUE)

{%- if execute -%}
{{ log("PIT " ~ this ~ " successfully cleaned!", True) }}
{%- endif -%}

{%- endmacro -%}

{%- macro snowflake__clean_up_pit(snapshot_relation, snapshot_trigger_column, sdts) -%}

DELETE FROM {{ this }} pit
WHERE pit.{{ sdts }} NOT IN (SELECT {{ sdts }} FROM {{ ref(snapshot_relation) }} snap WHERE {{ snapshot_trigger_column }}=TRUE)

{%- if execute -%}
{{ log("PIT " ~ this ~ " successfully cleaned!", True) }}
{%- endif -%}

{%- endmacro -%}

{%- macro exasol__clean_up_pit(snapshot_relation, snapshot_trigger_column, sdts) -%}

DELETE FROM {{ this }} pit
WHERE pit.{{ sdts }} NOT IN (SELECT {{ sdts }} FROM {{ ref(snapshot_relation) }} snap WHERE {{ snapshot_trigger_column }}=TRUE)

{%- if execute -%}
{{ log("PIT " ~ this ~ " successfully cleaned!", True) }}
{%- endif -%}

{%- endmacro -%}


{%- macro synapse__clean_up_pit(snapshot_relation, snapshot_trigger_column, sdts) -%}

DELETE pit
FROM {{ this }} AS pit 
LEFT JOIN {{ ref(snapshot_relation) }} AS snap
ON pit.{{ sdts }} = snap.{{ sdts }} AND {{ snapshot_trigger_column }}=1
WHERE snap.{{ sdts }} IS NULL

{%- if execute -%}
{{ log("PIT " ~ this ~ " successfully cleaned!", True) }}
{%- endif -%}

{%- endmacro -%}


{%- macro postgres__clean_up_pit(snapshot_relation, snapshot_trigger_column, sdts) -%}

DELETE FROM {{ this }} pit
WHERE NOT EXISTS (SELECT 1 FROM {{ ref(snapshot_relation) }} snap WHERE pit.{{ sdts }} = snap.{{ sdts }} AND snap.{{ snapshot_trigger_column }}=TRUE)

{%- if execute -%}
{{ log("PIT " ~ this ~ " successfully cleaned!", True) }}
{%- endif -%}

{%- endmacro -%}


{%- macro redshift__clean_up_pit(snapshot_relation, snapshot_trigger_column, sdts) -%}

DELETE FROM {{ this }}
WHERE NOT EXISTS (SELECT 1 FROM {{ ref(snapshot_relation) }} WHERE {{ this }}.{{ sdts }} = {{ ref(snapshot_relation) }}.{{ sdts }} AND {{ ref(snapshot_relation) }}.{{ snapshot_trigger_column }}=TRUE)


{%- if execute -%}
{{ log("PIT " ~ this ~ " successfully cleaned!", True) }}
{%- endif -%}

{%- endmacro -%}