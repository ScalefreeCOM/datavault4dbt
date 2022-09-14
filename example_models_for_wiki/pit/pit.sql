{{ config(materialized='incremental',
          unique_key='hk_account_d',
          post_hook="{{ datavault4dbt.clean_up_pit('control_snap_v0') }}") }}

{%- set yaml_metadata -%}
pit_type: 'Regular PIT'
tracked_entity: 'account_h'
hashkey: 'hk_account_h'
sat_names:
    - account_lroc_p_s
    - account_lroc_n_s
    - account_hroc_p_s
    - account_hroc_n_s
snapshot_relation: 'control_snap_v1'
snapshot_trigger_column: 'is_active'
dimension_key: 'hk_account_d'
custom_rsrc: 'PIT table for SAP/Accounts. For more information see our Wiki!'
{%- endset -%}    

{%- set metadata_dict = fromyaml(yaml_metadata) -%}

{%- set pit_type = metadata_dict['pit_type'] -%}
{%- set tracked_entity = metadata_dict['tracked_entity'] -%}
{%- set hashkey = metadata_dict['hashkey'] -%}
{%- set sat_names = metadata_dict['sat_names'] -%}
{%- set snapshot_relation = metadata_dict['snapshot_relation'] -%}
{%- set snapshot_trigger_column = metadata_dict['snapshot_trigger_column'] -%}
{%- set dimension_key = metadata_dict['dimension_key'] -%}
{%- set custom_rsrc = metadata_dict['custom_rsrc'] -%}

{{ datavault4dbt.control_snap_v0(pit_type=pit_type,
                                 tracked_entity=tracked_entity,
                                 hashkey=hashkey,
                                 sat_names=sat_names,
                                 snapshot_relation=snapshot_relation,
                                 snapshot_trigger_column=snapshot_trigger_column,
                                 dimension_key=dimension_key,
                                 custom_rsrc=custom_rsrc) }}
--------------------------------------------------------------------------                                 
For Description see macro file. Point out the optional parameters and what the dimension_key attribute affects here. 
Also link to the post_hook for PIT clean up, since it is used here (line 3 applies the post hook on this macro.)