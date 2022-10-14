
{#
    This macro calculates the load end dates for multi active data, based on a multi active attribute. It must be based on a version 0
    multi-active satellite, that would then hold multiple records per hashkey+ldts combination.

    Features:
        - Calculates virtualized load-end-dates to correctly identify multiple active records per batch
        - Enforces insert-only approach by view materialization
        - Allows multiple attributes to be used as the multi-active-attribute

    Parameters:

    sat_v0::string                              Name of the underlying version 0 multi-active satellite.

                                                Examples:
                                                    'contact_phonenumber_0_s'   This satellite would be the version 1 satellite of the underlying
                                                                                version 0 phone number satellite for contacts.

    hashkey::string                             Name of the parent hashkey column inside the version 0 satellite. Would either be the hashkey of a
                                                hub or a link. Needs to be similar to the 'parent_hashkey' parameter inside the sat_v0 model.

                                                Examples:
                                                    'hk_contact_h'          The satellite would be attached to the hub contact, which has the
                                                                            column 'hk_contact_h' as a hashkey column.

                                                    'hk_order_contact_l'    The satellite would be attached to the link between order and contact,
                                                                            which has the column 'hk_order_contact_l' as a hashkey column.

    hashdiff::string                            Name of the hashdiff column inside the underlying version 0 satellite. Needs to be similar to the
                                                'src_hashdiff' parameter inside the sat_v0 model. Must not include the ma_attribute in calculation.

                                                Examples:
                                                    'hd_contact_phonenumber_s'      Since we recommend naming the hashdiff column similar to the name
                                                                                    of the satellite entity, just with a prefix, this would be the
                                                                                    hashdiff column of the phone number satellite for contacts.

    ma_attribute::string|list of strings        Name of the multi active attribute inside the v0 satellite. This needs to be identified under the
                                                requirement that the combination of hashkey + ldts + ma_attribute is unique over the entire stage / satellite.

                                                Examples:
                                                    'phone_type'            Phone numbers are a good example of multi active data. One person could have an unlimited
                                                                            number of phone numbers, i. e. a mobile phone number, a home phone number, and a work phone
                                                                            number. therefore a contact has one active phone number per type per ldts and the phone_type
                                                                            uniquely identifies a record inside a hashkey+ldts combination.

                                                    ['phone_type', 'iid']   If a contact could have multiple mobile phone numbers, the phone_type alone would not be
                                                                            enough to uniquely identify a record inside a hashkey+ldts combination. Additionally the attribute
                                                                            iid, which is an increasing identifier within a phone_type, is added as a ma_attribute.


    src_ldts::string                            Name of the ldts column inside the source models. Is optional, will use the global variable 'datavault4dbt.ldts_alias'.
                                                Needs to use the same column name as defined as alias inside the staging model.

    src_rsrc::string                            Name of the rsrc column inside the source models. Is optional, will use the global variable 'datavault4dbt.rsrc_alias'.
                                                Needs to use the same column name as defined as alias inside the staging model.

    ledts_alias::string                         Desired alias for the load end date column. Is optional, will use the global variable 'datavault4dbt.ledts_alias' if
                                                set here.

    add_is_current_flag::boolean                Optional parameter to add a new column to the v1 sat based on the load end date timestamp (ledts). Default is false. If
                                                set to true it will add this is_current flag to the v1 sat. For each record this column will be set to true if the load
                                                end date time stamp is equal to the variable end of all times. If its not, then the record is not current therefore it
                                                will be set to false.

#}

{%- macro ma_sat_v1(sat_v0, hashkey, hashdiff, ma_attribute, src_ldts=none, src_rsrc=none, ledts_alias=none, add_is_current_flag=false) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts, src_rsrc, and ledts_alias are not set. #}

    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}
    {%- set ledts_alias = datavault4dbt.replace_standard(ledts_alias, 'datavault4dbt.ledts_alias', 'ledts') -%}

    {{ adapter.dispatch('ma_sat_v1', 'datavault4dbt')(sat_v0=sat_v0,
                                                      hashkey=hashkey,
                                                      hashdiff=hashdiff,
                                                      ma_attribute=ma_attribute,
                                                      src_ldts=src_ldts,
                                                      src_rsrc=src_rsrc,
                                                      ledts_alias=ledts_alias,
                                                      add_is_current_flag=add_is_current_flag) }}

{%- endmacro -%}
