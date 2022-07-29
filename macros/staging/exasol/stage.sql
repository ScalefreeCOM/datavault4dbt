{%- macro exasol__stage(include_source_columns,
                ldts,
                rsrc,  
                source_model,
                hashed_columns, 
                derived_columns, 
                ranked_columns, 
                sequence,
                prejoined_columns,
                missing_columns) -%}

{% if (source_model is none) and execute %}

    {%- set error_message -%}
    Staging error: Missing source_model configuration. A source model name must be provided.
    e.g.
    [REF STYLE]
    source_model: model_name
    OR
    [SOURCES STYLE]
    source_model:
        source_name: source_table_name
    {%- endset -%}

    {{- exceptions.raise_compiler_error(error_message) -}}
{%- endif -%}

{#- Check for source format or ref format and create relation object from source_model -#}
{% if source_model is mapping and source_model is not none -%}

    {%- set source_name = source_model | first -%}
    {%- set source_table_name = source_model[source_name] -%}

    {%- set source_relation = source(source_name, source_table_name) -%}
    {%- set all_source_columns = dbtvault_scalefree.source_columns(source_relation=source_relation) -%}
{%- elif source_model is not mapping and source_model is not none -%}
    {%- set source_relation = ref(source_model) -%}
    {%- set all_source_columns = dbtvault_scalefree.source_columns(source_relation=source_relation) -%}
{%- else -%}

    {%- set all_source_columns = [] -%}
{%- endif -%}   

{%- set ldts_rsrc_input_column_names = [] -%}

{# Setting the column name for load date timestamp and record source to the alias coming from the attributes #}
{%- set load_datetime_col_name = ldts.alias -%}
{%- set record_source_col_name = rsrc.alias -%}

{%- if dbtvault_scalefree.is_attribute(ldts.value) -%}
  {%- set ldts_rsrc_input_column_names = ldts_rsrc_input_column_names + [ldts.value]  -%}
{%- endif %}

{%- if dbtvault_scalefree.is_attribute(rsrc.value) -%}
  {%- set ldts_rsrc_input_column_names = ldts_rsrc_input_column_names + [rsrc.value] -%}
{%- endif %}

{%- if sequence is not none -%}  
  {%- set ldts_rsrc_input_column_names = ldts_rsrc_input_column_names + [sequence] -%}
{%- endif -%}

{%- set ldts = dbtvault_scalefree.as_constant(ldts.value) -%}
{%- set rsrc = dbtvault_scalefree.as_constant(rsrc.value) -%}

{# Getting the column names for all additional columns #}
{%- set derived_column_names = dbtvault_scalefree.extract_column_names(derived_columns) -%}
{%- set hashed_column_names = dbtvault_scalefree.extract_column_names(hashed_columns) -%}
{%- set ranked_column_names = dbtvault_scalefree.extract_column_names(ranked_columns) -%}
{%- set prejoined_column_names = dbtvault_scalefree.extract_column_names(prejoined_columns) -%}
{%- set missing_column_names = dbtvault_scalefree.extract_column_names(missing_columns) -%}
{%- set exclude_column_names = derived_column_names + hashed_column_names + prejoined_column_names + missing_column_names + ldts_rsrc_input_column_names %}
{%- set source_and_derived_column_names = (all_source_columns + derived_column_names) | unique | list -%}


{%- set source_columns_to_select = dbtvault_scalefree.process_columns_to_select(all_source_columns, exclude_column_names) | list -%}
{%- set derived_columns_to_select = dbtvault_scalefree.process_columns_to_select(source_and_derived_column_names, hashed_column_names) | unique | list -%}
{%- set final_columns_to_select = [] -%}

{%- set final_columns_to_select = final_columns_to_select + source_columns_to_select -%}
{%- set derived_columns_with_datatypes = dbtvault_scalefree.derived_columns_datatypes(derived_columns, source_relation) -%}
{# {{log("derived columns with updated datatypes " ~ derived_columns_with_datatypes, true)}} #}

{# {{log("derived_columns_with_datatypes_DICT: " ~ fromjson(derived_columns_with_datatypes) ,true)}} #}
{%- set derived_columns_with_datatypes_DICT = fromjson(derived_columns_with_datatypes) -%}


{#- Select hashing algorithm -#}

{%- set hash = var('dbtvault_scalefree.hash', 'MD5') -%}
{%- set hash_alg, unknown_key, error_key = dbtvault_scalefree.hash_default_values(hash_function=hash) -%}

{# Select timestamp and format variables #}

{%- set beginning_of_all_times = var('dbtvault_scalefree.beginning_of_all_times', '0001-01-01T00-00-01') -%}
{%- set end_of_all_times = var('dbtvault_scalefree.end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('dbtvault_scalefree.timestamp_format', 'YYYY-mm-ddTHH-MI-SS') -%}

{#- Setting the column name for load date time and record source if its not set  -#}

{% if load_datetime_col_name is none %}
  {% set load_datetime_col_name = var('dbtvault_scalefree.ldts', 'LDTS') %}
{% endif %}
{% if record_source_col_name is none %}
  {% set record_source_col_name = var('dbtvault_scalefree.rsrc', 'RSRC') %}
{% endif %}

{# Setting the error/unknown value for the record source  for the ghost records#}
{% set error_value_rsrc = var('dbtvault_scalefree.error_value_rsrc', 'ERROR') %}
{% set unknown_value_rsrc = var('dbtvault_scalefree.unknown_value_rsrc', 'SYSTEM') %}

WITH

source_data AS (
    SELECT

    {{- "\n\n    " ~ dbtvault_scalefree.print_list(dbtvault_scalefree.escape_column_names(all_source_columns)) if all_source_columns else " *" }}

  FROM {{ source_relation }}

  {% set last_cte = "source_data" -%}
),


{% set alias_columns = [load_datetime_col_name, record_source_col_name] %}

{#  Selecting all columns from the source data, renaming load date and record source to Scalefree naming conventions #}

ldts_rsrc_data AS (
  SELECT

  {{ ldts }} AS {{ load_datetime_col_name}},
  {{ rsrc }} AS {{record_source_col_name}},
  {% if sequence is not none -%}
    {{ sequence }} AS edwSequence,
    {%- set alias_columns = alias_columns + ['edwSequence'] -%}
  {% endif -%}

  {{ dbtvault_scalefree.print_list(dbtvault_scalefree.escape_column_names(source_columns_to_select)) }}

  FROM {{ last_cte }}

  {% set last_cte = "ldts_rsrc_data" %}
  {%- set final_columns_to_select = alias_columns + final_columns_to_select  %}
),

{% if dbtvault_scalefree.is_something(missing_columns) %}

{# Filling missing columns with NULL values for schema changes #}
missing_columns AS (

  SELECT 

    {{ dbtvault_scalefree.print_list(dbtvault_scalefree.escape_column_names(final_columns_to_select)) }},

  {%- for col, dtype in missing_columns.items() %}
    CAST(NULL as {{ dtype }}) as "{{ col }}",
    
  {% endfor %}

  FROM {{ last_cte }}
  {%- set last_cte = "missing_columns" -%}
  {%- set final_columns_to_select = final_columns_to_select + missing_column_names %}
),
{%- endif -%}

{% if dbtvault_scalefree.is_something(prejoined_columns) %}
{#  Prejoining Business Keys of other source objects for Link purposes #}
prejoined_columns AS (

  SELECT

  {{ dbtvault_scalefree.print_list(dbtvault_scalefree.prefix(columns=dbtvault_scalefree.escape_column_names(final_columns_to_select), prefix_str='lcte').split(',')) }}

  {%- for col, vals in prejoined_columns.items() -%}
    ,pj_{{loop.index}}.{{ vals['bk'] }} AS "{{ col }}"
  {% endfor -%}

  FROM {{ last_cte }} lcte

  {%- for col, vals in prejoined_columns.items() %}
    left join {{ source(vals['src_schema']|string, vals['src_table']) }} as pj_{{loop.index}} on lcte.{{ vals['this_column_name'] }} = pj_{{loop.index}}.{{ vals['ref_column_name'] }}
  {% endfor %}

  {% set last_cte = "prejoined_columns" -%}
  {%- set final_columns_to_select = final_columns_to_select + prejoined_column_names %}
),
{%- endif -%}

{%- if dbtvault_scalefree.is_something(derived_columns) %}
{# Adding derived columns to the selection #} 
derived_columns AS (

    SELECT

    {{ dbtvault_scalefree.print_list(dbtvault_scalefree.escape_column_names(final_columns_to_select)) }},

    {{ dbtvault_scalefree.derive_columns(columns=derived_columns) | indent(4) }}

    FROM {{ last_cte }}
    {%- set last_cte = "derived_columns" -%}
    {%- set final_columns_to_select = final_columns_to_select + derived_column_names %}
),
{%- endif -%}

{% if dbtvault_scalefree.is_something(hashed_columns) and hashed_columns is mapping -%}
{# Generating Hashed Columns (hashkeys and hashdiffs for Hubs/Links/Satellites) #} 
hashed_columns AS (

    SELECT

    {{ dbtvault_scalefree.print_list(dbtvault_scalefree.escape_column_names(final_columns_to_select)) }},

    {% set processed_hash_columns = dbtvault_scalefree.process_hash_column_excludes(hashed_columns) -%}
    {{- dbtvault_scalefree.hash_columns(columns=processed_hash_columns) | indent(4) }}

    FROM {{ last_cte }}
    {%- set last_cte = "hashed_columns" -%}
    {%- set final_columns_to_select = final_columns_to_select + hashed_column_names %}
),
{%- endif -%}

{% if dbtvault_scalefree.is_something(ranked_columns) -%}
{# Adding Ranked Columns to the selection #} 
ranked_columns AS (

    SELECT 
    {{ dbtvault_scalefree.print_list(dbtvault_scalefree.escape_column_names(final_columns_to_select)) }},

    {{ dbtvault_scalefree.rank_columns(columns=ranked_columns) | indent(4) if dbtvault_scalefree.is_something(ranked_columns) }}

    FROM {{ last_cte }}
    {%- set last_cte = "ranked_columns" -%}
    {%- set final_columns_to_select = final_columns_to_select + ranked_column_names %}
),
{%- endif -%}

{# Creating Ghost Record for unknown case, based on datatype #}
unknown_values AS (
    {%- set all_columns = adapter.get_columns_in_relation( source_relation ) -%}

    SELECT

    {{ dbtvault_scalefree.string_to_timestamp( timestamp_format , beginning_of_all_times) }} as {{load_datetime_col_name}}, 
    '{{ unknown_value_rsrc }}' as {{record_source_col_name}},
    {# Generating Ghost Records for all source columns, except the ldts, rsrc & edwSequence column #}
    {% for column in all_columns -%}
      {%- if column.name not in exclude_column_names %}
          {{ dbtvault_scalefree.ghost_record_per_datatype(column_name=column.name, datatype=column.dtype, ghost_record_type='unknown') }}
          {%- if not loop.last -%},{% endif %}
      {% endif -%}
    {% endfor %}

    {%- if  dbtvault_scalefree.is_something(missing_columns) -%},
      {# Additionally generating ghost record for Missing columns #}
      {% for col, dtype in missing_columns.items() -%}
        {{ dbtvault_scalefree.ghost_record_per_datatype(column_name=col, datatype=dtype, ghost_record_type='unknown') }}
        {%- if not loop.last -%},{% endif %}
      {% endfor %}
    {%- endif -%}



    {% if dbtvault_scalefree.is_something(prejoined_columns) -%}

      {# Additionally generating ghost records for Prejoined columns #}
      {% for col, vals in prejoined_columns.items() -%}
        {%- set pj_relation_columns = adapter.get_columns_in_relation( source(vals['src_schema']|string, vals['src_table']) ) -%}
        
          {% for column in pj_relation_columns -%}
            {% if column.name|lower == vals['bk']|lower -%},
              {{ dbtvault_scalefree.ghost_record_per_datatype(column_name=column.name, datatype=column.dtype, ghost_record_type='unknown') }}
            {%- endif -%}
          {% endfor -%}
        
        {% endfor -%}

    {%- endif %}

    {%- if dbtvault_scalefree.is_something(derived_columns) -%}
      {# Additionally generating Ghost Records for Derived Columns #}
      ,{% for column_name, properties in derived_columns_with_datatypes_DICT.items() -%}

        {{ dbtvault_scalefree.ghost_record_per_datatype(column_name=column_name, datatype=properties.datatype, ghost_record_type='unknown') }}
        {% if not loop.last -%},{%- endif -%}

      {% endfor %}
    {% endif %}
    ,{%- for hash_column in processed_hash_columns %}
    CAST('{{ unknown_key }}' as HASHTYPE) as "{{ hash_column }}"{{ "," if not loop.last }}
        
    {%- endfor %}
    
    ),

{# Creating Ghost Record for error case, based on datatype #}
error_values AS (
    {%- set all_columns = adapter.get_columns_in_relation( source_relation ) -%}

    SELECT
    
    {{ dbtvault_scalefree.string_to_timestamp( timestamp_format , end_of_all_times) }} as {{load_datetime_col_name}},
    '{{error_value_rsrc}}' as {{record_source_col_name}},

    {# Generating Ghost Records for Source Columns #}
    {% for column in all_columns -%}
        {%- if column.name not in exclude_column_names %}
          {{ dbtvault_scalefree.ghost_record_per_datatype(column_name=column.name, datatype=column.dtype, ghost_record_type='error') }}
          {%- if not loop.last -%},{% endif %}
        {% endif %}
    {% endfor %}

    {% if dbtvault_scalefree.is_something(missing_columns) -%},
      {# Additionally generating ghost record for Missing columns #}
      {% for col, dtype in missing_columns.items() -%}
        {{ dbtvault_scalefree.ghost_record_per_datatype(column_name=col, datatype=dtype, ghost_record_type='error') }}
          {%- if not loop.last -%},{% endif %}
      {% endfor %}
    {%- endif -%}

    {% if dbtvault_scalefree.is_something(prejoined_columns) -%}
      {# Additionally generating ghost records for the Prejoined columns #}
      {% for col, vals in prejoined_columns.items() -%}
        {% set pj_relation_columns = adapter.get_columns_in_relation( source(vals['src_schema']|string, vals['src_table']) ) -%}
        
        ,{% for column in pj_relation_columns -%}
          {%- if column.name|lower == vals['bk']|lower -%}
            {{ dbtvault_scalefree.ghost_record_per_datatype(column_name=column.name, datatype=column.dtype, ghost_record_type='error') }}
          {%- endif -%}
        {% endfor -%}

      {% endfor -%}

    {%- endif %}

    {%- if dbtvault_scalefree.is_something(derived_columns) -%}
      {# Additionally generating Ghost Records for Derived Columns #}
      ,{% for column_name, properties in derived_columns_with_datatypes_DICT.items() -%}

        {{ dbtvault_scalefree.ghost_record_per_datatype(column_name=column_name, datatype=properties.datatype, ghost_record_type='error') }}
        {% if not loop.last -%},{%- endif -%}

      {% endfor %}
    {% endif %}
    ,{%- for hash_column in processed_hash_columns %}
      CAST('{{ error_key }}' as HASHTYPE) as "{{ hash_column }}"{{ "," if not loop.last }}
        
    {%- endfor %}
    ),

{# Combining all previous ghost record calculations to two rows with the same width as regular entries #} 
ghost_records AS (
    SELECT * FROM unknown_values
    UNION ALL
    SELECT * FROM error_values
),

{# Combining the two ghost records with the regular data #} 
columns_to_select AS (

    SELECT

    *

    FROM {{ last_cte }}
    UNION ALL 
    SELECT * FROM ghost_records
)

SELECT * FROM columns_to_select

{%- endmacro -%}