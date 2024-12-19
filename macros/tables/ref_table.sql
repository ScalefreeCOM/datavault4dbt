{#
Example models:

Fully historized:

    {{ config(schema='core', materialized='view') }}

    {%- set yaml_metadata -%}
    ref_hub: 'nation_rh'
    ref_satellites: 
        - nation_rs1
        - nation_p_rs
    historized: 'full'
    {%- endset -%}

    {% set metadata_dict = fromyaml(yaml_metadata) %}

    {{ datavault4dbt.ref_table(ref_hub=metadata_dict['ref_hub'],
                        ref_satellites=metadata_dict['ref_satellites'],
                        historized=metadata_dict['historized'],
                        snapshot_relation=metadata_dict['snapshot_relation']) }}

Only latest data:

    {{ config(schema='core', materialized='view') }}

    {%- set yaml_metadata -%}
    ref_hub: 'nation_rh'
    ref_satellites: 
        - nation_rs1
        - nation_p_rs
    historized: 'latest'
    {%- endset -%}

    {% set metadata_dict = fromyaml(yaml_metadata) %}

    {{ datavault4dbt.ref_table(ref_hub=metadata_dict['ref_hub'],
                        ref_satellites=metadata_dict['ref_satellites'],
                        historized=metadata_dict['historized'],
                        snapshot_relation=metadata_dict['snapshot_relation']) }}

Snapshot Based:

    {{ config(schema='core', materialized='view') }}

    {%- set yaml_metadata -%}
    ref_hub: 'nation_rh'
    ref_satellites: 
        - nation_rs1
        - nation_p_rs
    historized: 'snapshot'
    snapshot_relation: snap_v1
    {%- endset -%}

    {% set metadata_dict = fromyaml(yaml_metadata) %}

    {{ datavault4dbt.ref_table(ref_hub=metadata_dict['ref_hub'],
                        ref_satellites=metadata_dict['ref_satellites'],
                        historized=metadata_dict['historized'],
                        snapshot_relation=metadata_dict['snapshot_relation']) }}

Include / Exclude per Satellite:

    {{ config(schema='core', materialized='view') }}

    {%- set yaml_metadata -%}
    ref_hub: 'nation_rh'
    ref_satellites: 
        nation_rs1:
            exclude:
                - N_NAME
        nation_p_rs:
            include:
                - N_NAME
    historized: 'full'
    {%- endset -%}

    {% set metadata_dict = fromyaml(yaml_metadata) %}

    {{ datavault4dbt.ref_table(ref_hub=metadata_dict['ref_hub'],
                        ref_satellites=metadata_dict['ref_satellites'],
                        historized=metadata_dict['historized'],
                        snapshot_relation=metadata_dict['snapshot_relation']) }}


#}


{%- macro ref_table(yaml_metadata=none, ref_hub=none, ref_satellites=none, src_ldts=none, src_rsrc=none, historized='latest', snapshot_relation=none, snapshot_trigger_column=none) -%}

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

    {%- set ref_hub =  datavault4dbt.yaml_metadata_parser(name='ref_hub', yaml_metadata=yaml_metadata, parameter=ref_hub, required=True, documentation=ref_hub_description) -%}
    {%- set ref_satellites =  datavault4dbt.yaml_metadata_parser(name='ref_satellites', yaml_metadata=yaml_metadata, parameter=ref_satellites, required=True, documentation=ref_satellites_description) -%}
    {%- set src_ldts =  datavault4dbt.yaml_metadata_parser(name='src_ldts', yaml_metadata=yaml_metadata, parameter=src_ldts, required=False, documentation=src_ldts_description) -%}
    {%- set src_rsrc =  datavault4dbt.yaml_metadata_parser(name='src_rsrc', yaml_metadata=yaml_metadata, parameter=src_rsrc, required=False, documentation=src_rsrc_description) -%}
    {%- set historized =  datavault4dbt.yaml_metadata_parser(name='historized', yaml_metadata=yaml_metadata, parameter=historized, required=False, documentation=historized_description) -%}
    {%- set snapshot_relation =  datavault4dbt.yaml_metadata_parser(name='snapshot_relation', yaml_metadata=yaml_metadata, parameter=snapshot_relation, required=False, documentation=snapshot_relation_description) -%}
    {%- set snapshot_trigger_column =  datavault4dbt.yaml_metadata_parser(name='snapshot_trigger_column', yaml_metadata=yaml_metadata, parameter=snapshot_trigger_column, required=False, documentation=snapshot_trigger_column_description) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts and src_rsrc are not set. #}

    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}
    {%- set snapshot_trigger_column = datavault4dbt.replace_standard(snapshot_trigger_column, 'datavault4dbt.snapshot_trigger_column', 'is_active') -%}

    {{ return(adapter.dispatch('ref_table', 'datavault4dbt')(ref_hub=ref_hub,
                                                            src_ldts=src_ldts,
                                                            src_rsrc=src_rsrc,
                                                            ref_satellites=ref_satellites,
                                                            historized=historized,
                                                            snapshot_relation=snapshot_relation,
                                                            snapshot_trigger_column=snapshot_trigger_column)) }}

{%- endmacro -%}
