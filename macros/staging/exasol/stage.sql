{%- macro exasol__stage(include_source_columns,
                ldts,
                rsrc,  
                source_name,
                source_table, 
                hashed_columns, 
                derived_columns, 
                ranked_columns, 
                sequence,
                prejoined_columns,
                missing_columns) -%}

{%- set source_relation = source(source_name|string, source_table) -%}
{%- set all_source_columns = dbtvault_scalefree.source_columns(source_relation=source_relation) -%}   

{%- set ldts_rsrc_column_names = [] -%}
{%- if ldts['is_available'] -%}
  {%- set ldts_rsrc_column_names = ldts_rsrc_column_names + [ldts['column']]  -%}
{%- endif -%}
{%- if rsrc['is_available'] -%}
  {%- set ldts_rsrc_column_names = ldts_rsrc_column_names + [rsrc['column']] -%}
{%- endif -%}
{%- if sequence is not none -%}  
  {%- set ldts_rsrc_column_names = ldts_rsrc_column_names + [sequence] -%}
{%- endif -%}

{%- set derived_column_names = dbtvault_scalefree.extract_column_names(derived_columns) -%}
{%- set hashed_column_names = dbtvault_scalefree.extract_column_names(hashed_columns) -%}
{%- set ranked_column_names = dbtvault_scalefree.extract_column_names(ranked_columns) -%}
{%- set prejoined_column_names = dbtvault_scalefree.extract_column_names(prejoined_columns) -%}
{%- set missing_column_names = dbtvault_scalefree.extract_column_names(missing_columns) -%}
{%- set exclude_column_names = derived_column_names + hashed_column_names + prejoined_column_names + missing_column_names + ldts_rsrc_column_names %}
{%- set source_and_derived_column_names = (all_source_columns + derived_column_names) | unique | list -%}


{%- set source_columns_to_select = dbtvault_scalefree.process_columns_to_select(all_source_columns, exclude_column_names) | list -%}
{%- set derived_columns_to_select = dbtvault_scalefree.process_columns_to_select(source_and_derived_column_names, hashed_column_names) | unique | list -%}
{%- set final_columns_to_select = [] -%}

{%- set final_columns_to_select = final_columns_to_select + source_columns_to_select -%}

{#- Select hashing algorithm -#}
{%- set hash = var('hash', 'MD5') -%}
{%- set hash_alg, unknown_key, error_key = dbtvault_scalefree.hash_default_values(hash_function=hash) -%}

{%- set beginning_of_all_times = var('beginning_of_all_times', '0001-01-01T00-00-01') -%}
{%- set end_of_all_times = var('end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('timestamp_format', 'YYYY-mm-ddTHH-MI-SS') -%}

WITH

source_data AS (
    SELECT

    {{- "\n\n    " ~ dbtvault_scalefree.print_list(dbtvault_scalefree.escape_column_names(all_source_columns)) if all_source_columns else " *" }}

  FROM {{ source_relation }}

  {% set last_cte = "source_data" -%}
),


{% set alias_columns = ['LDTS', 'RSRC'] %}
-- Selecting all columns from the source data, renaming load date and record source to Scalefree naming conventions
ldts_rsrc_data AS (
  SELECT

  {% if ldts['is_available'] -%}
    {{ ldts['column'] }} as LDTS,
  {% else -%}
    '{{ ldts["value"] }}' as LDTS,
  {% endif -%}
  {% if rsrc['is_available'] -%}
    {{ rsrc['column'] }} as RSRC,
  {% else -%}
    '{{ rsrc["value"] }}' as RSRC,
  {% endif -%}
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


-- Filling missing columns with NULL values for schema changes
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

-- Prejoining Business Keys of other source objects for Link purposes
{% if dbtvault_scalefree.is_something(prejoined_columns) %}

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
-- Adding derived columns to the selection
derived_columns AS (

    SELECT

    {{ dbtvault_scalefree.print_list(dbtvault_scalefree.escape_column_names(final_columns_to_select)) }},

    {{ dbtvault_scalefree.derive_columns(columns=derived_columns) | indent(4) }}

    FROM {{ last_cte }}
    {%- set last_cte = "derived_columns" -%}
    {%- set final_columns_to_select = final_columns_to_select + derived_column_names %}
),
{%- endif -%}

-- Generating Hashed Columns (hashkeys and hashdiffs for Hubs/Links/Satellites)
{% if dbtvault_scalefree.is_something(hashed_columns) and hashed_columns is mapping -%}

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

-- Adding Ranked Columns to the selection
{% if dbtvault_scalefree.is_something(ranked_columns) -%}

ranked_columns AS (

    SELECT *,

    {{ dbtvault_scalefree.rank_columns(columns=ranked_columns) | indent(4) if dbtvault_scalefree.is_something(ranked_columns) }}

    FROM {{ last_cte }}
    {%- set last_cte = "ranked_columns" -%}
    {%- set final_columns_to_select = final_columns_to_select + ranked_column_names %}
),
{%- endif -%}

-- Creating Ghost Record for unknown case, based on datatype
unknown_values AS (
    {%- set all_columns = adapter.get_columns_in_relation( source_relation ) -%}

    SELECT

    {{ dbtvault_scalefree.string_to_timestamp( timestamp_format , beginning_of_all_times) }} as LDTS, 
    'SYSTEM' as RSRC,
    --Generating Ghost Records for all source columns, except the ldts, rsrc & edwSequence column
    {% for column in all_columns -%}
      {%- if column.name not in exclude_column_names %}
          {{ dbtvault_scalefree.ghost_record_per_datatype(column_name=column.name, datatype=column.dtype, ghost_record_type='unknown') }}
          {%- if not loop.last %},{% endif -%}
      {% endif -%}
    {% endfor %}

    {%- if missing_columns is not none -%},
    --Additionally generating ghost record for missing columns
      {% for col, dtype in missing_columns.items() %}
        {{ dbtvault_scalefree.ghost_record_per_datatype(column_name=col, datatype=dtype, ghost_record_type='unknown') }}
        {%- if not loop.last %},{% endif -%}
      {% endfor %}
    {%- endif -%}



    {% if prejoined_columns is not none -%}
    --Additionally generating ghost records for the prejoined attributes
      {% for col, vals in prejoined_columns.items() %}
        {%- set pj_relation_columns = adapter.get_columns_in_relation( source(vals['src_schema']|string, vals['src_table']) ) -%}
        
          {% for column in pj_relation_columns -%}
            {% if column.name|lower == vals['bk']|lower -%},
              {{ dbtvault_scalefree.ghost_record_per_datatype(column_name=column.name, datatype=column.dtype, ghost_record_type='unknown') }}
            {%- endif -%}
          {% endfor -%}
        
        {% endfor -%}

    {%- endif %}

    {%- if derived_columns is not none -%}
    --Additionally generating Ghost Records for Derived Columns
      ,{% for column_name, properties in derived_columns.items() -%}
        {{ dbtvault_scalefree.ghost_record_per_datatype(column_name=column_name, datatype=properties.datatype, ghost_record_type='unknown') }}
        {%- if not loop.last %},{% endif -%}
      {% endfor %}
    {% endif %}

    ,{%- for hash_column in processed_hash_columns %}
    CAST('{{ unknown_key }}' as HASHTYPE) as "{{ hash_column }}"{{ "," if not loop.last }}
        
    {%- endfor %}
    
    ),

--Creating Ghost Record for error case, based on datatype
error_values AS (
    {%- set all_columns = adapter.get_columns_in_relation( source_relation ) -%}

    SELECT
    
    {{ dbtvault_scalefree.string_to_timestamp( timestamp_format , end_of_all_times) }} as LDTS,
    'ERROR' as RSRC,

    -- Generating Ghost Records for Source Columns
    {% for column in all_columns -%}
        {%- if column.name not in exclude_column_names %}
          {{ dbtvault_scalefree.ghost_record_per_datatype(column_name=column.name, datatype=column.dtype, ghost_record_type='error') }}
          {%- if not loop.last %},{% endif -%}
        {% endif %}
    {% endfor %}

    --Additionally generating ghost record for missing columns
    {% if missing_columns is not none -%},
      {% for col, dtype in missing_columns.items() %}
        {{ dbtvault_scalefree.ghost_record_per_datatype(column_name=col, datatype=dtype, ghost_record_type='error') }}
        {%- if not loop.last %},{% endif -%}
      {% endfor %}
    {%- endif -%}

    --Additionally generating ghost records for the prejoined attributes
    {% if prejoined_columns is not none -%}

      {% for col, vals in prejoined_columns.items() %}
        {% set pj_relation_columns = adapter.get_columns_in_relation( source(vals['src_schema']|string, vals['src_table']) ) -%}
        
        ,{% for column in pj_relation_columns -%}
          {%- if column.name|lower == vals['bk']|lower -%}
            {{ dbtvault_scalefree.ghost_record_per_datatype(column_name=column.name, datatype=column.dtype, ghost_record_type='error') }}
          {%- endif -%}
        {% endfor -%}

      {% endfor -%}

    {%- endif %}

    --Additionally generating Ghost Records for Derived Columns
    {% if derived_columns is not none -%},
      {% for column_name, properties in derived_columns.items() -%}
        {{ dbtvault_scalefree.ghost_record_per_datatype(column_name=column_name, datatype=properties.datatype, ghost_record_type='error') }}
        {%- if not loop.last %},{% endif -%}
      {% endfor %}
    {% endif %}

    ,{%- for hash_column in processed_hash_columns %}
    CAST('{{ error_key }}' as HASHTYPE) as "{{ hash_column }}"{{ "," if not loop.last }}
        
    {%- endfor %}
    ),

-- Combining all previous ghost record calculations to two rows with the same width as regular entries
ghost_records AS (
    SELECT * FROM unknown_values
    UNION ALL
    SELECT * FROM error_values
),

-- Combining the two ghost records with the regular data
columns_to_select AS (

    SELECT

    *

    FROM {{ last_cte }}
    UNION ALL 
    SELECT * FROM ghost_records
)

SELECT * FROM columns_to_select

{%- endmacro -%}