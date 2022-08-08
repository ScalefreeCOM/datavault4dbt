{#
    This Macro creates a virtual satellite for hubs, that creates a timeline in which each Driving Key has one active relationship
    at a time. Therefor it calculates a start_timestamp and end_timestamp for each entry in the connected link.

    Parameters:

    link::string                    Name of the link model this sat should be attached to.
    link_hk::string                 Name of the PK column of the link. The Hashkey column over all Business Keys.
    driving_key::string             Name of the Driving Key column inside the link.
    sec_fks::string|list(string)    Name(s) of all secondary foreign key columns inside the link. Would be all foreign keys except the Driving Key. 
    ldts::string                    Name of the ldts column inside the link
    rsrc::string                    Name of the rsrc column inside the link

#}


{%- macro link_eff_sat(link, link_hk, driving_key, sec_fks, ldts, rsrc) -%}

    {{ return(adapter.dispatch('link_eff_sat', 'dbtvault_scalefree')(link=link, link_hk=link_hk, driving_key=driving_key, 
                                                                     sec_fks=sec_fks, ldts=ldts, rsrc=rsrc)) }}

{%- endmacro -%}                                                                     


{%- macro default__link_eff_sat(link, link_hk, driving_key, sec_fks, ldts, rsrc) -%}

{{- dbtvault_scalefree.check_required_parameters(link=link, link_hk=link_hk, driving_key=driving_key,
                                                sec_fks=sec_fks, ldts=ldts, rsrc=rsrc) -}}

{%- set source_cols = dbtvault_scalefree.expand_column_list(columns=[hashkey_column, driving_key, sec_fks, ldts, rsrc]) -%}

{%- set link_relation = ref(link) -%}

{%- set start_date_alias = var('dbtvault_scalefree.start_date_alias', 'valid_from') -%}
{%- set end_date_alias = var('dbtvault_scalefree.end_date_alias', 'valid_to') -%}

{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', '%Y-%m-%dT%H-%M-%S') -%}

{%- if not dbtvault_scalefree.is_list(sec_fks) -%}
    {%- set sec_fks = [sec_fks] -%}
{%- endif -%}

{{ dbtvault_scalefree.prepend_generated_by() }}

WITH 

eff_sat AS (

    SELECT 
        {{ link_hk }},
        {{ driving_key }},
        {%- for fk in sec_fks %}
            {{ fk }},
        {% endfor -%}
        {{ rsrc }},
        {{ ldts }} AS {{ start_date_alias }},
        COALESCE(LEAD(TIMESTAMP_SUB({{ ldts }}, INTERVAL 1 MICROSECOND)) OVER (PARTITION BY {{ driving_key }} ORDER BY {{ ldts }}),{{ dbtvault_scalefree.string_to_timestamp( timestamp_format , end_of_all_times) }}) as {{ end_date_alias }}
    FROM {{ link_relation }} --from stage

)

SELECT * FROM eff_sat

{%- endmacro -%}