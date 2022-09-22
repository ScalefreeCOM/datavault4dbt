{#
    This macro should be used as a post hook for each PIT table, whenever a logarithmic snapshot logic is used.
    The macro deletes all records in a PIT table, that are no longer active. Deletion is no problem here because
    no actual data is deleted, only pointers to satellite entries.
    
    Parameters:
        snapshot_relation::string       The name of the dbt model that creates the snapshot table / view, that has
                                        the logarithmic snapshot logic applied.
                                        
    Example Usage:

        An example usage for applying this macro as a post hook for a PIT table would look like this inside the PIT source_models
        config block: 

            "{{ config(post_hook="{{ dbtvault_scalefree.clean_up_pit('control_snap_view') }}") }}"

#}


{%- macro clean_up_pit(snapshot_relation) -%}

{{ return(adapter.dispatch('clean_up_pit', 'dbtvault_scalefree')(snapshot_relation=snapshot_relation)) }}

{%- endmacro -%}

{%- macro default__clean_up_pit(snapshot_relation) -%}

DELETE {{ this }} pit
WHERE pit.sdts not in (SELECT sdts FROM {{ ref(snapshot_relation) }} snap WHERE is_active=TRUE)

{{ log("PIT " ~ this ~ " successfully cleaned!", True) }}

{%- endmacro -%}

{%- macro snowflake__clean_up_pit(snapshot_relation) -%}

DELETE FROM {{ this }} pit
WHERE pit.sdts NOT IN (SELECT sdts FROM {{ ref(snapshot_relation) }} snap WHERE is_active=TRUE)

{{ log("PIT " ~ this ~ " successfully cleaned!", True) }}

{%- endmacro -%}
