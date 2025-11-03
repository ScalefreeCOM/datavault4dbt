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
#}

{%- macro pit(yaml_metadata=none, tracked_entity=none, hashkey=none, sat_names=none, snapshot_relation=none, dimension_key=none, snapshot_trigger_column=none, ldts=none, custom_rsrc=none, ledts=none, sdts=none, pit_type=none, refer_to_ghost_records=True) -%}

    {% set tracked_entity_description = "
    tracked_entity::string              Name of the tracked Hub entity. Must be available as a model inside the dbt project.
    " %}

    {% set hashkey_description = "
    hashkey::string                     The name of the hashkey column inside the previously referred Hub entity.
    " %}

    {% set sat_names_description = "
    sat_names::list of strings          A list of all the satellites that should be included in this PIT table. Can only be satellites
                                        that are attached to the tracked Hub, and should typically include all those satellites.
                                        You should always refer here to the version 1 satellites, since those hold the load-end-date.
                                        The macro currently supports regular satellites and nh-satellites.
    " %}

    {% set snapshot_relation_description = "
    snapshot_relation::string           The name of the snapshot relation. It needs to be available as a model inside this dbt project.
    " %}

    {% set snapshot_trigger_column_description = "
    snapshot_trigger_column::string     The name of the column inside the previously mentioned snapshot relation, that is boolean and
                                        identifies the snapshots that should be included in the PIT table.
    " %}

    {% set dimension_key_description = "
    dimension_key::string               The desired name of the dimension key inside the PIT table. Should follow some naming conventions.
                                        Recommended is the name of the hashkey with a '_d' suffix.
    " %}

    {% set ldts_description = "
    ldts::string                        Name of the ldts column inside all source models. Is optional, will use the global variable
                                        'datavault4dbt.ldts_alias'. Needs to use the same column name as defined as alias inside the staging model.
    " %}

    {% set custom_rsrc_description = "
    custom_rsrc::string                 A custom string that should be inserted into the 'rsrc' column inside the PIT table. Since
                                        a PIT table is a business vault entity, the technical record source is no longer used here.
                                        Is optional, if not defined, no column is added.
    " %}

    {% set ledts_description = "
    ledts::string                       Name of the load-end-date column inside the satellites. Is optional, will use the global variable
                                        'datavault4dbt.ledts_alias' if not set here.
    " %}

    {% set sdts_description = "
    sdts::string                        Name of the snapshot date timestamp column inside the snapshot table. It is optional, will use the 
                                        global variable 'datavault4dbt.sdts_alias' if not set here.
    " %}

    {% set pit_type_description = "
    pit_type::string                    String to insert into the 'pit_type' column. Has to be prefixed by a !.
                                        Allows for future implementations of other PIT variants, like T-PITs etc.
                                        Can be set freely, something like 'PIT' could be the default. 
                                        Is optional, if not set, no column will be added.
    " %}

    {% set refer_to_ghost_records_description = "
    refer_to_ghost_records::boolean     Value to define if a NULL satellite hashkey should be replaced by the unknown key.
                                        Optional parameter, default is True.
    " %}

    {%- set tracked_entity          = datavault4dbt.yaml_metadata_parser(name='tracked_entity', yaml_metadata=yaml_metadata, parameter=tracked_entity, required=True, documentation=tracked_entity_description) -%}
    {%- set hashkey                 = datavault4dbt.yaml_metadata_parser(name='hashkey', yaml_metadata=yaml_metadata, parameter=hashkey, required=True, documentation=hashkey_description) -%}
    {%- set sat_names               = datavault4dbt.yaml_metadata_parser(name='sat_names', yaml_metadata=yaml_metadata, parameter=sat_names, required=True, documentation=sat_names_description) -%}
    {%- set snapshot_relation       = datavault4dbt.yaml_metadata_parser(name='snapshot_relation', yaml_metadata=yaml_metadata, parameter=snapshot_relation, required=True, documentation=snapshot_relation_description) -%}
    {%- set dimension_key           = datavault4dbt.yaml_metadata_parser(name='dimension_key', yaml_metadata=yaml_metadata, parameter=dimension_key, required=True, documentation=dimension_key_description) -%}
    {%- set snapshot_trigger_column = datavault4dbt.yaml_metadata_parser(name='snapshot_trigger_column', yaml_metadata=yaml_metadata, parameter=snapshot_trigger_column, required=False, documentation=snapshot_trigger_column_description) -%}
    {%- set ldts                    = datavault4dbt.yaml_metadata_parser(name='ldts', yaml_metadata=yaml_metadata, parameter=ldts, required=False, documentation=ldts_description) -%}
    {%- set custom_rsrc             = datavault4dbt.yaml_metadata_parser(name='custom_rsrc', yaml_metadata=yaml_metadata, parameter=custom_rsrc, required=False, documentation=custom_rsrc_description) -%}
    {%- set ledts                   = datavault4dbt.yaml_metadata_parser(name='ledts', yaml_metadata=yaml_metadata, parameter=ledts, required=False, documentation=ledts_description) -%}
    {%- set sdts                    = datavault4dbt.yaml_metadata_parser(name='sdts', yaml_metadata=yaml_metadata, parameter=sdts, required=False, documentation=sdts_description) -%}
    {%- set pit_type                = datavault4dbt.yaml_metadata_parser(name='pit_type', yaml_metadata=yaml_metadata, parameter=pit_type, required=False, documentation=pit_type_description) -%}
    {%- set refer_to_ghost_records  = datavault4dbt.yaml_metadata_parser(name='refer_to_ghost_records', yaml_metadata=yaml_metadata, parameter=refer_to_ghost_records, required=False, documentation=pit_type_description) -%}

    {# Applying the default aliases as stored inside the global variables, if ldts, sdts and ledts are not set. #}

    {%- set ldts = datavault4dbt.replace_standard(ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set ledts = datavault4dbt.replace_standard(ledts, 'datavault4dbt.ledts_alias', 'ledts') -%}
    {%- set sdts = datavault4dbt.replace_standard(sdts, 'datavault4dbt.sdts_alias', 'sdts') -%}

    {%- if var('datavault4dbt.use_premium_package', False) == True -%}
        {{ datavault4dbt_premium_package.insert_metadata_pit(pit_type=pit_type,
                                                        tracked_entity=tracked_entity,
                                                        hashkey=hashkey,
                                                        sat_names=sat_names,
                                                        ldts=ldts,
                                                        sdts=sdts,
                                                        custom_rsrc=custom_rsrc,
                                                        ledts=ledts,
                                                        snapshot_relation=snapshot_relation,
                                                        snapshot_trigger_column=snapshot_trigger_column,
                                                        dimension_key=dimension_key,
                                                        refer_to_ghost_records=refer_to_ghost_records) }}
    {%- endif %}
    
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
                                                        dimension_key=dimension_key,
                                                        refer_to_ghost_records=refer_to_ghost_records)) }}

{%- endmacro -%}
