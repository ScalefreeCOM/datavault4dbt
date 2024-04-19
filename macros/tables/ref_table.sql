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


{%- macro ref_table(ref_hub, ref_satellites, src_ldts=none, src_rsrc=none, historized='latest', snapshot_relation=none, snapshot_trigger_column=none) -%}
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
