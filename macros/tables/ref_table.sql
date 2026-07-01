{%- macro ref_table(yaml_metadata=none, ref_hub=none, ref_satellites=none, src_ldts=none, src_rsrc=none, historized='latest', snapshot_relation=none, snapshot_trigger_column=none, include_business_objects_before_appearance=none) -%}

    {% set ref_hub_description = "
    ref_hub::string     Name of the underlying ref_hub model.
    " %}

    {% set ref_satellites_description = "
    ref_satellites::string|list of strings      Name(s) of the reference satellites to be included in this ref_table. Optional: 'include' & 'exclude' as dictionary keys for each satellite.
    " %}

    {% set src_ldts_description = "
    src_ldts::string                Name of the ldts column inside the source models. Is optional, will use the global variable 'datavault4dbt.ldts_alias'.
                                    Needs to use the same column name as defined as alias inside the staging model.
    " %}

    {% set src_rsrc_description = "
    src_rsrc::string                Name of the rsrc column inside the source models. Is optional, will use the global variable 'datavault4dbt.rsrc_alias'.
                                    Needs to use the same column name as defined as alias inside the staging model.
    " %}

    {% set historized_description = "
    historized::string              Possible values are 'full', 'latest', or 'snapshot'. Influences how much history this reference table will hold. 
    " %}

    {% set snapshot_relation_description = "
    snapshot_relation::string       Only required, if 'historized' set to 'snapshot'. Name of the snapshot_v1 model to be used. 
    " %}

    {% set snapshot_trigger_column_description = "
    snapshot_trigger_column::string     Only required, if 'historized' set to 'snapshot'. Defaults to global variable 'datavault4dbt.sdts_alias'. Only needs to be set if alias deviates from global variable.
    " %}

    {% set include_business_objects_before_appearance_description = "
    include_business_objects_before_appearance::boolean  Controls whether ref_table rows are emitted for snapshots older than a hub row's
                                                         ldts.
                                                           - True                        : emit ref_table rows for every snapshot, including
                                                                                           snapshots before the ref_hub row's ldts.
                                                           - False (default for ref_table): filter out ref_table rows where h.ldts > snap.sdts;
                                                                                           a ref_hub row appears in the ref_table only for
                                                                                           snapshots at or after its own ldts.
                                                         Overrides the global variable 'datavault4dbt.ref_table__include_business_objects_before_appearance'
                                                         for this model. Optional parameter.
    " %}

    {%- set ref_hub                 = datavault4dbt.yaml_metadata_parser(name='ref_hub', yaml_metadata=yaml_metadata, parameter=ref_hub, required=True, documentation=ref_hub_description) -%}
    {%- set ref_satellites          = datavault4dbt.yaml_metadata_parser(name='ref_satellites', yaml_metadata=yaml_metadata, parameter=ref_satellites, required=True, documentation=ref_satellites_description) -%}
    {%- set src_ldts                = datavault4dbt.yaml_metadata_parser(name='src_ldts', yaml_metadata=yaml_metadata, parameter=src_ldts, required=False, documentation=src_ldts_description) -%}
    {%- set src_rsrc                = datavault4dbt.yaml_metadata_parser(name='src_rsrc', yaml_metadata=yaml_metadata, parameter=src_rsrc, required=False, documentation=src_rsrc_description) -%}
    {%- set historized              = datavault4dbt.yaml_metadata_parser(name='historized', yaml_metadata=yaml_metadata, parameter=historized, required=False, documentation=historized_description) -%}
    {%- set snapshot_relation       = datavault4dbt.yaml_metadata_parser(name='snapshot_relation', yaml_metadata=yaml_metadata, parameter=snapshot_relation, required=False, documentation=snapshot_relation_description) -%}
    {%- set snapshot_trigger_column = datavault4dbt.yaml_metadata_parser(name='snapshot_trigger_column', yaml_metadata=yaml_metadata, parameter=snapshot_trigger_column, required=False, documentation=snapshot_trigger_column_description) -%}
    {%- set include_business_objects_before_appearance = datavault4dbt.yaml_metadata_parser(name='include_business_objects_before_appearance', yaml_metadata=yaml_metadata, parameter=include_business_objects_before_appearance, required=False, documentation=include_business_objects_before_appearance_description) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts and src_rsrc are not set. #}

    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}
    {%- set snapshot_trigger_column = datavault4dbt.replace_standard(snapshot_trigger_column, 'datavault4dbt.snapshot_trigger_column', 'is_active') -%}

    {# Resolve include_business_objects_before_appearance: parameter -> global var -> default False. #}
    {%- if include_business_objects_before_appearance is none -%}
        {%- set include_business_objects_before_appearance = var('datavault4dbt.ref_table__include_business_objects_before_appearance', false) -%}
    {%- endif -%}

    {# For Fusion static_analysis overwrite #}
    {% set static_analysis_config = datavault4dbt.get_static_analysis_config('ref_table') %}
    {%- if datavault4dbt.is_something(static_analysis_config) -%}
        {{ config(static_analysis='off') }}
    {%- endif -%}

    {%- if var('datavault4dbt.use_premium_package', False) == True -%}
        {{ datavault4dbt_premium_package.insert_metadata_ref_table(ref_hub=ref_hub,
                                                            src_ldts=src_ldts,
                                                            src_rsrc=src_rsrc,
                                                            ref_satellites=ref_satellites,
                                                            historized=historized,
                                                            snapshot_relation=snapshot_relation,
                                                            snapshot_trigger_column=snapshot_trigger_column,
                                                            include_business_objects_before_appearance=include_business_objects_before_appearance) }}
    {%- endif %}

    {{ return(adapter.dispatch('ref_table', 'datavault4dbt')(ref_hub=ref_hub,
                                                            src_ldts=src_ldts,
                                                            src_rsrc=src_rsrc,
                                                            ref_satellites=ref_satellites,
                                                            historized=historized,
                                                            snapshot_relation=snapshot_relation,
                                                            snapshot_trigger_column=snapshot_trigger_column,
                                                            include_business_objects_before_appearance=include_business_objects_before_appearance)) }}

{%- endmacro -%}
