
{#
    This macro calulates the load end dates for multi active data, based on a multi active attribute. It must be based on a regular
    version 0 satellite, that would then hold multiple records per hashkey+ldts combination. You have to identify one or more attributes
    inside the source, that in combination with the hashkey/business key will uniquely identify a record.

    Parameters:

    sat_v0::string              Name of the underlying version 0 satellite. 

                                Examples:
                                    'currency_rates_0_s'        This satellite would be the version 1 satellite of the underlying
                                                                version 0 rates satellite for currencies.

    hashkey::string             Name of the parent hashkey column inside the version 0 satellite. Would either be the hashkey of a
                                hub or a link. Needs to be similar to the 'parent_hashkey' parameter inside the sat_v0 model.

                                Examples: 
                                    'hk_currency_h'         The satellite would be attached to the hub currency, which has the
                                                            column 'hk_currency_h' as a hashkey column.

                                    'hk_currency_country_l' The satellite would be attached to the link between currency and country,
                                                            which has the column 'hk_currency_country_l' as a hashkey column.

    hashdiff::string            Name of the hashdiff column inside the underlying version 0 satellite. Needs to be similar to the
                                'src_hashdiff' pararmeter inside the sat_v0 model. 

                                Examples:
                                    'hd_currency_rates_s'   Since we recommend naming the hashdiff column similar to the name
                                                            of the satellite entity, just with a prefix, this would be the
                                                            hashdiff column of the rates satellite for currency.

    ma_attribute::string        Name of the multi active attribute inside the v0 satellite. This needs to be identified under the
                                requirement that the combination of hashkey + ldts + ma_attribute is unique over the entire stage / satellite.

                                Examples:
                                    'currency_isocode'      Currency data is a good example for multi active data. My source data holds
                                                            one row for each isocode+ldts combination.

    src_ldts::string            Name of the ldts column inside the source models. Is optional, will use the global variable 'dbtvault_scalefree.ldts_alias'.
                                Needs to use the same column name as defined as alias inside the staging model.

    src_rsrc::string            Name of the rsrc column inside the source models. Is optional, will use the global variable 'dbtvault_scalefree.rsrc_alias'.
                                Needs to use the same column name as defined as alias inside the staging model.

    ledts_alias::string         Desired alias for the load end date column. Is optional, will use the global variable 'dbtvault_scalefree.ledts_alias' if
                                set here.  
#}

{%- macro ma_sat_v1(sat_v0, hashkey, hashdiff, ma_attribute, src_ldts='ldts', ledts_alias='ledts') -%}

    {{ adapter.dispatch('ma_sat_v1', 'dbtvault_scalefree')(sat_v0=sat_v0,
                                         hashkey=hashkey,
                                         hashdiff=hashdiff,
                                         ma_attribute=ma_attribute,
                                         src_ldts=src_ldts,
                                         ledts_alias=ledts_alias) }}

{%- endmacro -%}