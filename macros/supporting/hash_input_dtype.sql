{#
    Returns the string datatype that hash inputs are casted to, before they are handed over to the hash function.

    Two different casts exist, which can be configured independently:
      - 'attribute': The cast of one single column inside `attribute_standardise`.
      - 'concat':    The cast of the fully concatenated payload inside `concattenated_standardise`.

    Multi Active Satellites are deliberately NOT covered here. Their payload is aggregated across all
    active records of one group before it is hashed, so a shortened datatype is far more likely to be
    exceeded there. `multi_active_concattenated_standardise` therefore keeps its hardcoded datatype.

    CAUTION: Choosing a datatype that is shorter than the actual hash input leads to a silent
    truncation on most adapters, which changes the resulting hash values.
#}

{%- macro hash_input_dtype(type='concat') %}

    {{ return(adapter.dispatch('hash_input_dtype', 'datavault4dbt')(type=type)) }}

{%- endmacro -%}


{%- macro default__hash_input_dtype(type) %}

    {%- if type == 'attribute' -%}
        {%- set var_name = 'datavault4dbt.hash_input_attribute_dtype' -%}
        {%- set fallbacks = {"bigquery": "STRING", "snowflake": "STRING", "exasol": "VARCHAR(20000) UTF8", "postgres": "VARCHAR", "synapse": "VARCHAR(4000)", "fabric": "VARCHAR(4000)", "oracle": "VARCHAR2(2000)", "databricks": "STRING", "trino": "VARCHAR", "sqlserver": "VARCHAR(MAX)"} -%}
    {%- else -%}
        {%- set var_name = 'datavault4dbt.hash_input_concat_dtype' -%}
        {%- set fallbacks = {"bigquery": "STRING", "snowflake": "STRING", "exasol": "VARCHAR(2000000) UTF8", "postgres": "VARCHAR", "redshift": "VARCHAR", "synapse": "VARCHAR(4000)", "fabric": "VARCHAR(4000)", "oracle": "VARCHAR2(2000)", "databricks": "STRING", "trino": "VARCHAR", "sqlserver": "VARCHAR(MAX)"} -%}
    {%- endif -%}

    {%- set global_var = var(var_name, none) -%}

    {%- if global_var is mapping and target.type in global_var.keys()|map('lower') -%}

        {%- set hash_input_dtype = global_var[target.type] -%}

    {%- elif global_var is not mapping and datavault4dbt.is_something(global_var) -%}

        {%- set hash_input_dtype = global_var -%}

    {%- else -%}

        {%- set hash_input_dtype = fallbacks.get(target.type, 'STRING') -%}

        {%- if execute -%}
            {%- do exceptions.warn("Warning: Adapter '"~ target.type ~"' not found in '" ~ var_name ~ "' variable. Defaulting to '" ~ hash_input_dtype ~ "'.") -%}
        {%- endif -%}

    {%- endif -%}

    {{ return(hash_input_dtype) }}

{%- endmacro -%}
