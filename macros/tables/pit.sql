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

{%- macro pit(yaml_metadata=none, tracked_entity=none, hashkey=none, sat_names=none, snapshot_relation=none, dimension_key=none, snapshot_trigger_column=none, ldts=none, custom_rsrc=none, ledts=none, sdts=none, pit_type=none, refer_to_ghost_records=True, snapshot_optimization=False, include_business_objects_before_appearance=none) -%}

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

    {%- set include_business_objects_before_appearance_description = "
    include_business_objects_before_appearance::boolean  Controls whether ghost PIT rows are emitted for snapshots older than a hub row's
                                                         ldts.
                                                           - True  (default for PIT): emit ghost PIT rows for every active snapshot,
                                                                                      including snapshots before hub.ldts.
                                                           - False                  : filter out PIT rows where te.ldts > snap.sdts;
                                                                                      a hub row appears in the PIT only for snapshots
                                                                                      at or after its own ldts.
                                                         Overrides the global variable 'datavault4dbt.pit__include_business_objects_before_appearance'
                                                         for this model. Optional parameter.
    " -%}

    {%- set snapshot_optimization_description = "
    snapshot_optimization::boolean|string   Controls which snapshots are processed in incremental runs. Accepts a boolean or a mode string.
                                            Modes:
                                              - False / 'off'        (default): process all (active) snapshots; deduplicate new rows against
                                                                                existing dimension keys in the target. Safe, most expensive.
                                              - 'hwm'                         : process only snapshots with sdts > MAX(sdts) in the target
                                                                                (High Water Mark). Cheapest; Supported on all adapters.
                                              - True / 'relevant'             : process new snapshots + update the last processed snapshot
                                                                                per satellite to catch late-arriving satellite data. Requires
                                                                                the model to be configured with a unique_key constraint since
                                                                                existing rows may be updated. Snowflake only; using this on
                                                                                other adapters raises a compile-time error.
                                            Logic description:
                                              - False / 'off': rebuilds PIT records for every (active) snapshot
                                                from the current source state and only skips rows whose dimension_key is already in the
                                                target.
                                              - 'hwm' only processes snapshots with sdts > MAX(sdts) in the target and never revisits
                                                already-processed snapshots. So anything that would change the PIT content for an old
                                                snapshot is missed:
                                                  Example 1: New Hub Row
                                                     A new business key appears in the Hub. Under the PIT default of
                                                     'datavault4dbt.pit__include_business_objects_before_appearance' = true, full-refresh
                                                     emits PIT rows (ghost sat rows) for that hub for every active snapshot, including
                                                     snapshots older than hub.ldts. 'hwm' only emits rows for snapshots > MAX(sdts) and
                                                     therefore does not backfill the old snapshots.
                                                     e.g. MAX(sdts) in PIT = 2025-06-30. A new hub row is loaded today with
                                                          ldts = 2025-06-30.
                                                          Incremental 'hwm': no PIT rows for that hub-row for snapshots <= 2025-06-30.
                                                          Full-refresh:      ghost PIT rows for that hub-row for every active snapshot,
                                                                             including those before 2025-06-30.
                                                     Setting 'datavault4dbt.pit__include_business_objects_before_appearance' to False
                                                     (or passing include_business_objects_before_appearance=False to the model) aligns
                                                     full-refresh with 'hwm' as long as the hub is loaded strictly after every already
                                                     processed snapshot.
                                                  Example 2: Snapshot Trigger
                                                     A snapshot whose trigger is flipped from false to true after MAX(sdts) has moved
                                                     past it is skipped by 'hwm' entirely; full-refresh picks it up.
                                                     e.g. MAX(sdts) in PIT = 2025-06-30. The snapshot at sdts = 2025-05-15 originally had
                                                          is_active = false, so it was never processed. Someone now flips is_active = true
                                                          for 2025-05-15.
                                                          Incremental 'hwm': 2025-05-15 <= MAX(sdts), so it stays skipped.
                                                          Full-refresh:      2025-05-15 is picked up and materialized in the PIT.
                                              - True / 'relevant' (Snowflake only) processes new snapshots like 'hwm', and additionally
                                                updates the boundary snapshot (the last processed sdts per satellite) so that late-arriving
                                                satellite data whose ldts falls into an already-processed snapshot's range is picked up.
                                                Existing PIT rows for the re-swept snapshot are updated via the model's unique_key merge,
                                                so this mode requires the model to be configured with a unique_key constraint on the
                                                dimension_key. Note that this mode does NOT recover from cases (1) or (2) above.
                                            Optional parameter, default is 'off' / False.
    " -%}

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
    {%- set snapshot_optimization  = datavault4dbt.yaml_metadata_parser(name='snapshot_optimization', yaml_metadata=yaml_metadata, parameter=snapshot_optimization, required=False, documentation=snapshot_optimization_description) -%}
    {%- set include_business_objects_before_appearance = datavault4dbt.yaml_metadata_parser(name='include_business_objects_before_appearance', yaml_metadata=yaml_metadata, parameter=include_business_objects_before_appearance, required=False, documentation=include_business_objects_before_appearance_description) -%}

    {# Applying the default aliases as stored inside the global variables, if ldts, sdts and ledts are not set. #}

    {%- set ldts = datavault4dbt.replace_standard(ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set ledts = datavault4dbt.replace_standard(ledts, 'datavault4dbt.ledts_alias', 'ledts') -%}
    {%- set sdts = datavault4dbt.replace_standard(sdts, 'datavault4dbt.sdts_alias', 'sdts') -%}

    {# Resolve include_business_objects_before_appearance: parameter -> global var -> default True. #}
    {%- if include_business_objects_before_appearance is none -%}
        {%- set include_business_objects_before_appearance = var('datavault4dbt.pit__include_business_objects_before_appearance', true) -%}
    {%- endif -%}

        {# For Fusion static_analysis overwrite #}
    {% set static_analysis_config = datavault4dbt.get_static_analysis_config('pit') %}
    {%- if datavault4dbt.is_something(static_analysis_config) -%}
        {{ config(static_analysis='off') }}
    {%- endif -%}

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
                                                        refer_to_ghost_records=refer_to_ghost_records,
                                                        snapshot_optimization=snapshot_optimization,
                                                        include_business_objects_before_appearance=include_business_objects_before_appearance) }}
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
                                                        refer_to_ghost_records=refer_to_ghost_records,
                                                        snapshot_optimization=snapshot_optimization,
                                                        include_business_objects_before_appearance=include_business_objects_before_appearance)) }}

{%- endmacro -%}
