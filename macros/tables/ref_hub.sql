{%- macro ref_hub(ref_keys, source_models, src_ldts=none, src_rsrc=none) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts and src_rsrc are not set. #}

    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}

    {{ return(adapter.dispatch('ref_hub', 'datavault4dbt')(ref_keys=ref_keys,
                                                            src_ldts=src_ldts,
                                                            src_rsrc=src_rsrc,
                                                            source_models=source_models)) }}

{%- endmacro -%}