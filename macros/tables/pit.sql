{%- macro pit(pit_type, tracked_entity, hashkey, sat_names, snapshot_relation, snapshot_trigger_column, dimension_key, ldts=none, custom_rsrc=none, ledts=none) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts, src_rsrc, and ledts_alias are not set. #}
    
    {%- set ldts = dbtvault_scalefree.replace_standard(ldts, 'dbtvault_scalefree.ldts_alias', 'ldts') -%}
    {%- set ledts = dbtvault_scalefree.replace_standard(ledts, 'dbtvault_scalefree.ledts_alias', 'ledts') -%}

    {%- if custom_rsrc is none -%}
        {%- set custom_rsrc = 'PIT_' + tracked_entity|string -%}
    {%- endif -%}

    {{ return(adapter.dispatch('pit','dbtvault_scalefree')(pit_type=pit_type,
                                                        tracked_entity=tracked_entity,
                                                        hashkey=hashkey,
                                                        sat_names=sat_names,
                                                        ldts=ldts,
                                                        custom_rsrc=custom_rsrc,
                                                        ledts=ledts,
                                                        snapshot_relation=snapshot_relation,
                                                        snapshot_trigger_column=snapshot_trigger_column,
                                                        dimension_key=dimension_key)) }}

{%- endmacro -%}