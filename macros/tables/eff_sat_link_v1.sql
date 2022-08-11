{#
    This macro calculates the effective_from and effective_to timestamps of relationships based on an effectivity satellite
    version 0. This model is best materialized as a view, because otherwise it would require updates. For each version 0
    effectivity satellite for a link, one version 1 effectivity satellite should be created. 

    Parameters: 

    eff_sat_link_v0::string                     The name of the underlying version 0 effectivity satellite model.
                                                
                                                Examples:
                                                    'account_contact_sfdc_0_es'     The underlying v0 effectivity satellite for the
                                                                                    link between account and contact is referenced.
    
    link_hashkey::string                        Name of the hashkey column inside the v0 effectivity satellite. Must be the same as
                                                in the v0 effectivity satellite.

    driving_key::string | list of strings       Name(s) of the driving key column(s) inside the v0 effectivity satellite. 

    secondary_fks::string | list of strings     Name(s) of the secondary foreign key(s) inside the v0 effectivity satellite. 
    
    src_ldts::string                            Name of the ldts column inside the source models. Is optional, will use the global variable 'dbtvault_scalefree.ldts_alias'.
                                                Needs to use the same column name as defined as alias inside the staging model.

    src_rsrc::string                            Name of the rsrc column inside the source models. Is optional, will use the global variable 'dbtvault_scalefree.rsrc_alias'.
                                                Needs to use the same column name as defined as alias inside the staging model.

    eff_from_alias::string                      Desired alias of the effective_from column. Is optional, will use the global variable 'dbtvault_scalfree.eff_from_alias' if not set here.
    
    eff_to_alias::string                        Desired alias of the effective_to column. Is optional, will use the global variable 'dbtvault_scalfree.eff_to_alias' if not set here.


#}

{%- macro eff_sat_link_v1(eff_sat_link_v0, link_hashkey, driving_key, secondary_fks, src_ldts=none, src_rsrc=none, eff_from_alias=none, eff_to_alias=none) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts and src_rsrc are not set. #}
    
    {%- set src_ldts = dbtvault_scalefree.replace_standard(src_ldts, 'dbtvault_scalefree.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = dbtvault_scalefree.replace_standard(src_rsrc, 'dbtvault_scalefree.rsrc_alias', 'rsrc') -%}
    {%- set eff_from_alias = dbtvault_scalefree.replace_standard(src_rsrc, 'dbtvault_scalefree.eff_from_alias', 'effective_from') -%}
    {%- set eff_to_alias = dbtvault_scalefree.replace_standard(src_rsrc, 'dbtvault_scalefree.eff_to_alias', 'effective_to') -%}

    {{ return(adapter.dispatch('eff_sat_link_v1', 'dbtvault_scalefree')(eff_sat_link_v0=eff_sat_link_v0,
                                                                        link_hashkey=link_hashkey, 
                                                                        driving_key=driving_key,
                                                                        secondary_fks=secondary_fks,
                                                                        src_ldts=src_ldts,
                                                                        src_rsrc=src_rsrc,
                                                                        eff_from_alias=eff_from_alias,
                                                                        eff_to_alias=eff_to_alias)) }}

{%- endmacro -%}