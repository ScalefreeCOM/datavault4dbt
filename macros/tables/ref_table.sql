{%- macro ref_table(ref_hub, ref_satellites, src_ldts=none, src_rsrc=none, historized='latest', snapshot_relation=none, snapshot_trigger_column=none) -%}
    {# Applying the default aliases as stored inside the global variables, if src_ldts and src_rsrc are not set. #}

    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}
    {%- set snapshot_trigger_column = datavault4dbt.replace_standard(snapshot_trigger_column, 'datavault4dbt.rsrc_alias', 'rsrc') -%}

    {{ return(adapter.dispatch('ref_table', 'datavault4dbt')(ref_hub=ref_hub,
                                                            src_ldts=src_ldts,
                                                            src_rsrc=src_rsrc,
                                                            ref_satellites=ref_satellites,
                                                            historized=historized,
                                                            snapshot_relation=snapshot_relation)) }}

{%- endmacro -%}