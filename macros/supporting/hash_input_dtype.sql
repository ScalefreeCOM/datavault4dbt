{#
    Returns the string datatype that hash inputs are casted to, before they are handed over to the hash function.

    Three different casts exist, which can be configured independently:
      - 'attribute':           The cast of one single column inside `attribute_standardise`.
      - 'concat':              The cast of the fully concatenated payload inside `concattenated_standardise`.
      - 'multi_active_concat': The cast of the per-record payload inside `multi_active_concattenated_standardise`,
                               which determines the return type of the aggregate around it.

    Multi Active Satellites have their own variable because their payload is aggregated across all active
    records of one group before it is hashed, so a shortened datatype is exceeded far earlier there.
    Shortening `hash_input_concat_dtype` for performance must not silently break Multi Active hashes.

    CAUTION: Choosing a datatype that is shorter than the actual hash input leads to a silent
    truncation on most adapters, which changes the resulting hash values.
#}

{%- macro hash_input_dtype(type='concat') %}

    {{ return(adapter.dispatch('hash_input_dtype', 'datavault4dbt')(type=type)) }}

{%- endmacro -%}


{%- macro default__hash_input_dtype(type) %}

    {%- if type not in ['attribute', 'concat', 'multi_active_concat'] -%}
        {%- do exceptions.raise_compiler_error("hash_input_dtype: type must be 'attribute', 'concat' or 'multi_active_concat', got: " ~ type) -%}
    {%- endif -%}

    {#- 'concat' and 'multi_active_concat' share their fallbacks, they cast the same joined payload -#}
    {%- if type == 'attribute' -%}
        {%- set fallbacks = {"bigquery": "STRING", "snowflake": "STRING", "exasol": "VARCHAR(20000) UTF8", "postgres": "VARCHAR", "synapse": "VARCHAR(4000)", "fabric": "VARCHAR(4000)", "oracle": "VARCHAR2(2000)", "databricks": "STRING", "trino": "VARCHAR", "sqlserver": "VARCHAR(MAX)"} -%}
    {%- else -%}
        {%- set fallbacks = {"bigquery": "STRING", "snowflake": "STRING", "exasol": "VARCHAR(2000000) UTF8", "postgres": "VARCHAR", "redshift": "VARCHAR", "synapse": "VARCHAR(4000)", "fabric": "VARCHAR(4000)", "oracle": "VARCHAR2(2000)", "databricks": "STRING", "trino": "VARCHAR", "sqlserver": "VARCHAR(MAX)"} -%}
    {%- endif -%}

    {%- set var_name = 'datavault4dbt.hash_input_' ~ type ~ '_dtype' -%}
    {%- set global_var = var(var_name, none) -%}
    {%- set adapter_name = (target.type | lower) -%}
    {%- set hash_input_dtype = none -%}

    {%- if global_var is mapping -%}
        {%- set hash_input_dtype = global_var.get(adapter_name) -%}
        {%- if hash_input_dtype is none -%}
            {%- set hash_input_dtype = global_var.get(target.type) -%}
        {%- endif -%}
    {%- elif datavault4dbt.is_something(global_var) -%}
        {%- set hash_input_dtype = global_var -%}
    {%- endif -%}

    {%- if not datavault4dbt.is_something(hash_input_dtype) -%}
        {%- set hash_input_dtype = fallbacks.get(adapter_name, 'STRING') -%}
        {%- if execute and global_var is mapping -%}
            {%- do exceptions.warn("Warning: Adapter '" ~ target.type ~ "' not found in '" ~ var_name ~ "' variable. Defaulting to '" ~ hash_input_dtype ~ "'.") -%}
        {%- endif -%}
    {%- endif -%}

    {{ return(hash_input_dtype) }}

{%- endmacro -%}
