{#
    This macro creates a PIT table to gather snapshot based information about one hub and its surrounding satellites.
    For this macro to work, a snapshot table is required, that has a trigger column to identify which snapshots
    to include in the PIT table. The easiest way to create such a snapshot table is to use the control_snap macros
    provided by this package.

    Features:
        - Tracks the active satellite entries for each entry in a Hub for each snapshot
        - Strongly improves performance if upstream queries require many JOIN operations
        - Creates a unique dimension key to optimize loading performance of incremental loads
        - Allows to insert a static string as record source column, matching business vault definition of a record source

    Parameters:

    pit_type::string                    String to insert into the 'pit_type' column. Allows for future implementations of other
                                        PIT variants, like T-PITs etc. Can be set freely, something like 'PIT' could be the default. 
                                        Is optional, if not set, no column will be added.

    tracked_entity::string              Name of the tracked Hub entity. Must be available as a model inside the dbt project.

    hashkey::string                     The name of the hashkey column inside the previously referred Hub entity.

    sat_names::list of strings          A list of all the satellites that should be included in this PIT table. Can only be satellites
                                        that are attached to the tracked Hub, and should typically include all those satellites.
                                        You should always refer here to the version 1 satellites, since those hold the load-end-date.
                                        The macro currently supports regular satellites and nh-satellites.

    snapshot_relation::string           The name of the snapshot relation. It needs to be available as a model inside this dbt project.

    snapshot_trigger_column::string     The name of the column inside the previously mentioned snapshot relation, that is boolean and
                                        identifies the snapshots that should be included in the PIT table.

    dimension_key::string               The desired name of the dimension key inside the PIT table. Should follow some naming conventions.
                                        Recommended is the name of the hashkey with a '_d' suffix.

    ldts::string                        Name of the ldts column inside all source models. Is optional, will use the global variable
                                        'datavault4dbt.ldts_alias'. Needs to use the same column name as defined as alias inside the staging model.

    custom_rsrc::string                 A custom string that should be inserted into the 'rsrc' column inside the PIT table. Since
                                        a PIT table is a business vault entity, the technical record source is no longer used here.
                                        Is optional, if not defined, no column is added.

    ledts::string                       Name of the load-end-date column inside the satellites. Is optional, will use the global variable
                                        'datavault4dbt.ledts_alias' if not set here.
    
    sdts::string                        Name of the snapshot date timestamp column inside the snapshot table. It is optional, will use the 
                                        global variable 'datavault4dbt.sdts_alias' if not set here.

#}



{%- macro pit(tracked_entity, hashkey, sat_names, snapshot_relation, dimension_key, snapshot_trigger_column=none, ldts=none, custom_rsrc=none, ledts=none, sdts=none, pit_type=none) -%}

    {# Applying the default aliases as stored inside the global variables, if ldts, sdts and ledts are not set. #}

    {%- set ldts = datavault4dbt.replace_standard(ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set ledts = datavault4dbt.replace_standard(ledts, 'datavault4dbt.ledts_alias', 'ledts') -%}
    {%- set sdts = datavault4dbt.replace_standard(sdts, 'datavault4dbt.sdts_alias', 'sdts') -%}

    {{ return(adapter.dispatch('pit','datavault4dbt')(pit_type=pit_type,
                                                        tracked_entity=tracked_entity,
                                                        hashkey=hashkey,
                                                        sat_names=sat_names,
                                                        ldts=ldts,
                                                        sdts=sdts,
                                                        custom_rsrc=custom_rsrc,
                                                        ledts=ledts,
                                                        snapshot_relation=snapshot_relation,
                                                        snapshot_trigger_column=snapshot_trigger_column,
                                                        dimension_key=dimension_key)) }}

{%- endmacro -%}
