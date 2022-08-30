
{#
    This macro calulates the load end dates for multi active data, based on a multi active attribute. It must be based on a regular
    version 0 satellite, that would then hold multiple records per hashkey+ldts combination. You have to identify one or more attributes
    inside the source, that in combination with the hashkey/business key will uniquely identify a record.

    Features: 
        - Applies a multi-active logic on top of a regular version 0 satellite
        - Calculates virtualized load-end-dates to correctly identify multiple active records per batch
        - Enforces insert-only approach by view materialization
        - Allows multiple attributes to be used as the multi-active-attribute

    Parameters:

    sat_v0::string                              Name of the underlying version 0 satellite.

                                                Examples:
                                                    'contact_phonenumber_0_s'   This satellite would be the version 1 satellite of the underlying
                                                                                version 0 phonenumber satellite for contacts.

    hashkey::string                             Name of the parent hashkey column inside the version 0 satellite. Would either be the hashkey of a
                                                hub or a link. Needs to be similar to the 'parent_hashkey' parameter inside the sat_v0 model.

                                                Examples:
                                                    'hk_contact_h'          The satellite would be attached to the hub contact, which has the
                                                                            column 'hk_contact_h' as a hashkey column.

                                                    'hk_order_contact_l'    The satellite would be attached to the link between order and contact,
                                                                            which has the column 'hk_order_contact_l' as a hashkey column.

    hashdiff::string                            Name of the hashdiff column inside the underlying version 0 satellite. Needs to be similar to the
                                                'src_hashdiff' pararmeter inside the sat_v0 model. Must include the ma_attribute in calculation.

                                                Examples:
                                                    'hd_contact_phonenumber_s'      Since we recommend naming the hashdiff column similar to the name
                                                                                    of the satellite entity, just with a prefix, this would be the
                                                                                    hashdiff column of the phonenumber satellite for contacts.

    ma_attribute::string|list of strings        Name of the multi active attribute inside the v0 satellite. This needs to be identified under the
                                                requirement that the combination of hashkey + ldts + ma_attribute is unique over the entire stage / satellite.

                                                Examples:
                                                    'phone_type'            Phone numbers are a good example for multi active data. One person could have an unlimited
                                                                            number of phone numbers, i. e. a mobile phone number, a home phone number, and a work phone
                                                                            number. Therefor a contact has one active phone number per type per ldts and the phone_type
                                                                            uniquely identifies a record inside a hashkey+ldts combination.

                                                    ['phone_type', 'iid']   If a contact could have multiple mobile phonenumbers, the phone_type alone would not be
                                                                            enough to uniquely identify a record inside a hashkey+ldts combination. Additionally the attribute
                                                                            iid, which is an increasing identifier within a phone_type, is added as a ma_attribute.


    src_ldts::string                            Name of the ldts column inside the source models. Is optional, will use the global variable 'dbtvault_scalefree.ldts_alias'.
                                                Needs to use the same column name as defined as alias inside the staging model.

    src_rsrc::string                            Name of the rsrc column inside the source models. Is optional, will use the global variable 'dbtvault_scalefree.rsrc_alias'.
                                                Needs to use the same column name as defined as alias inside the staging model.

    ledts_alias::string                         Desired alias for the load end date column. Is optional, will use the global variable 'dbtvault_scalefree.ledts_alias' if
                                                set here.

#}

{%- macro ma_sat_v1(sat_v0, hashkey, hashdiff, ma_attribute, src_ldts=none, ledts_alias=none) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts, src_rsrc, and ledts_alias are not set. #}

    {%- set src_ldts = dbtvault_scalefree.replace_standard(src_ldts, 'dbtvault_scalefree.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = dbtvault_scalefree.replace_standard(src_rsrc, 'dbtvault_scalefree.rsrc_alias', 'rsrc') -%}
    {%- set src_ledts = dbtvault_scalefree.replace_standard(src_ledts, 'dbtvault_scalefree.ledts_alias', 'ledts') -%}

    {{ adapter.dispatch('ma_sat_v1', 'dbtvault_scalefree')(sat_v0=sat_v0,
                                         hashkey=hashkey,
                                         hashdiff=hashdiff,
                                         ma_attribute=ma_attribute,
                                         src_ldts=src_ldts,
                                         ledts_alias=ledts_alias) }}

{%- endmacro -%}
