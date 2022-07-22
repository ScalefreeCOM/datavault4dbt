{%- macro stage(rsrc, source_schema, source_table, ldts='ldts', hashed_columns=none, derived_columns=none, ranked_columns=none, sequence=none, prejoined_columns=none, missing_columns=none) -%}
    
    {{ return(adapter.dispatch('stage', 'dbtvault_scalefree')(ldts=ldts,
                                        rsrc=rsrc, 
                                        source_schema=source_schema,
                                        source_table=source_table, 
                                        hashed_columns=hashed_columns, 
                                        derived_columns=derived_columns, 
                                        ranked_columns=ranked_columns, 
                                        sequence=sequence,
                                        prejoined_columns=prejoined_columns,
                                        missing_columns=missing_columns)) }}

{%- endmacro -%}


{%- macro default__stage(ldts,
                rsrc,  
                source_schema,
                source_table, 
                hashed_columns, 
                derived_columns, 
                ranked_columns, 
                sequence,
                prejoined_columns,
                missing_columns) -%}

{%- set derived_column_names = dbtvault_scalefree.extract_column_names(derived_columns) -%}
{%- set hashed_column_names = dbtvault_scalefree.extract_column_names(hashed_columns) -%}
{%- set ranked_column_names = dbtvault_scalefree.extract_column_names(ranked_columns) -%}
{%- set exclude_column_names = derived_column_names + hashed_column_names %}

{#- Select hashing algorithm -#}
{%- set hash = var('hash', 'MD5') -%}
{%- if hash == 'MD5' -%}
    {%- set unknown_key = '00000000000000000000000000000000' -%}
    {%- set error_key = 'ffffffffffffffffffffffffffffffff' -%}
{%- elif hash == 'SHA' or hash == 'SHA1' -%}
    {%- set unknown_key = '0000000000000000000000000000000000000000' -%}
    {%- set error_key = 'ffffffffffffffffffffffffffffffffffffffff' -%}
{%- elif hash == 'SHA2' or hash == 'SHA256' -%}
    {%- set unknown_key = '0000000000000000000000000000000000000000000000000000000000000000' -%}
    {%- set error_key = 'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' -%}
{%- endif -%}

{%- set beginning_of_all_times = var('beginning_of_all_times', '0001-01-01T00-00-01') -%}
{%- set end_of_all_times = var('end_of_all_times', '8888-12-31T23-59-59') -%}
{%- set timestamp_format = var('timestamp_format', 'YYYY-MM-DDTHH-MI-SS') -%}

{%- set source_relation = source(source_schema, source_table) -%}

WITH

-- Selecting all columns from the source data, renaming load date and record source to Scalefree naming conventions
source_data AS (
  SELECT
  -- Checking if there is a column available for the load date time stamp in the source data:
    {% if ldts['is_available'] == false %}
      '{{ ldts["value"]|string }}'
    {% else -%}
      src.{{ ldts["column"]|string }}
    {%- endif %} as ldts ,
  -- Checking if there is a column available for rsrc in the source data
    {% if rsrc['is_available'] == false -%}
      '{{ rsrc["value"]|string }}'
    {% else -%}
      src.{{ rsrc['column']|string }}
    {%- endif %} as rsrc,
    {{ ',src.{{ sequence }} as edwSequence' if sequence is not none }}

    src.*

  FROM {{ source_relation }} as src

  {%- set last_cte = "source_data" -%}
),

{% if dbtvault_scalefree.is_something(missing_columns) %}

-- Filling missing columns with NULL values for schema changes
missing_columns AS (

  SELECT 

  {{last_cte}}.*,

  {%- for col, dtype in missing_columns.items() %}
    ,CAST(NULL as {{ dtype }}) as {{ col }}
  {% endfor %}

  FROM {{ last_cte }}
  {%- set last_cte = "missing_columns" -%}
),
{%- endif -%}

-- Prejoining Business Keys of other source objects for Link purposes
{% if dbtvault_scalefree.is_something(prejoined_columns) %}

prejoined_columns AS (  
  
  SELECT

  lcte.*

  {%- for col, vals in prejoined_columns.items() %}
    ,pj_{{loop.index}}.{{ vals['bk'] }} AS {{ col }}
  {% endfor -%}

  FROM {{ last_cte }} lcte

  {%- for col, vals in prejoined_columns.items() %}
    left join {{ source(vals['src_schema']|string, vals['src_table']) }} as pj_{{loop.index}} on lcte.{{ vals['this_column_name'] }} = pj_{{loop.index}}.{{ vals['ref_column_name'] }}
  {% endfor %}

  {% set last_cte = "prejoined_columns" -%}
),
{%- endif -%}


{%- if dbtvault_scalefree.is_something(derived_columns) %}
-- Adding derived columns to the selection
derived_columns AS (

    SELECT
    {{ last_cte }}.*,
    {{ dbtvault_scalefree.derive_columns(columns=derived_columns) | indent(4) }}

    FROM {{ last_cte }}
    {%- set last_cte = "derived_columns" -%}
),
{%- endif -%}

-- Generating Hashed Columns (hashkeys and hashdiffs for Hubs/Links/Satellites)
{% if dbtvault_scalefree.is_something(hashed_columns) and hashed_columns is mapping -%}

hashed_columns AS (

    SELECT

    {{last_cte}}.*,

    {% set processed_hash_columns = dbtvault_scalefree.process_hash_column_excludes(hashed_columns) -%}
    {{- dbtvault_scalefree.hash_columns(columns=processed_hash_columns) | indent(4) }}

    FROM {{ last_cte }}
    {%- set last_cte = "hashed_columns" -%}
),
{%- endif -%}

-- Adding Ranked Columsn to the selection
{% if dbtvault_scalefree.is_something(ranked_columns) -%}

ranked_columns AS (

    SELECT {{last_cte}}.*,

    {{ dbtvault_scalefree.rank_columns(columns=ranked_columns) | indent(4) if dbtvault_scalefree.is_something(ranked_columns) }}

    FROM {{ last_cte }}
    {%- set last_cte = "ranked_columns" -%}
),
{%- endif -%}

-- Creating Ghost Record for unknown case, based on datatype
unknown_values AS (
    {%- set all_columns = adapter.get_columns_in_relation( source(source_schema|string, source_table )) -%}
    {%- set special_columns = ['edwRecordSource', 'rsrc_file'] -%}

    SELECT
    {%- for column in all_columns -%}
        {%- if column.name in special_columns %} 'SYSTEM' as {{ column.name }}
        {% elif column.dtype == 'TIMESTAMP' %} {{ dbtvault_scalefree.string_to_timestamp(timestamp_format ,  beginning_of_all_times) }} as {{ column.name }}
        {% elif column.dtype == 'VARCHAR' %} '(unknown)' as {{ column.name }}
        {% elif column.dtype == 'DECIMAL' %} CAST('0' as DECIMAL) as {{ column.name }}
        {% elif column.dtype == 'DOUBLE PRECISION' %} CAST('0' as DOUBLE PRECISION) as {{ column.name }}
        {% elif column.dtype == 'BOOLEAN' %} CAST('FALSE' as BOOLEAN) as {{ column.name }}
        {% else %} CAST(NULL as {{ column.dtype }}) as {{ column.name }}
        {% endif -%}{%- if not loop.last %},{% endif -%}
    {% endfor %}

    --Additionally generating ghost records for the prejoined attributes
    {% if prejoined_columns is not none -%},
      {%- for col, vals in prejoined_columns.items() %}
        {%- set pj_relation_columns = adapter.get_columns_in_relation( source(vals['src_schema']|string, vals['src_table']) ) -%}
        
          {%- for column in pj_relation_columns -%}
            {%- if column.name|lower == vals['bk']|lower -%}
              {% if column.dtype == 'TIMESTAMP' %} {{ dbtvault_scalefree.string_to_timestamp(timestamp_format ,  beginning_of_all_times) }} as {{ col }}
              {% elif column.dtype == 'VARCHAR' %} '(unknown)' as {{ col }}
              {% elif column.dtype == 'DECIMAL' %} CAST('0' as DECIMAL) as {{ col }}
              {% elif column.dtype == 'DOUBLE PRECISION' %} CAST('0' as DOUBLE PRECISION) as {{ col }}
              {% elif column.dtype == 'BOOLEAN' %} CAST('FALSE' as BOOLEAN) as {{ col }}
              {% else %} CAST(NULL as {{ column.dtype }}) as {{ col }}
              {% endif -%}{%- if not loop.last %},{% endif -%}
            {%- endif -%}
          {% endfor -%}
        
        {% endfor -%}

    {%- endif %}
    ),

--Creating Ghost Record for error case, based on datatype
error_values AS (
    {%- set all_columns = adapter.get_columns_in_relation(source(source_schema|string, source_table)) -%}
    {% if rsrc['column'] is not none %}
      {%- set special_columns = [rsrc['column']] -%}
    {% else %}
      {%- set special_columns = [rsrc['value']] -%}
    {% endif %}

    SELECT
    {%- for column in all_columns -%}
        {%- if column.name in special_columns %} 'ERROR' as {{ column.name }}
        {% elif column.dtype == 'TIMESTAMP' %} {{ dbtvault_scalefree.string_to_timestamp(timestamp_format ,  end_of_all_times) }} as {{ column.name }}
        {% elif column.dtype == 'VARCHAR' %} '(error)' as {{ column.name }}
        {% elif column.dtype == 'DECIMAL' %} CAST('-1' as DECIMAL) as {{ column.name }}
        {% elif column.dtype == 'DOUBLE PRECISION' %} CAST('-1' as DOUBLE PRECISION) as {{ column.name }}
        {% elif column.dtype == 'BOOLEAN' %} CAST('FALSE' as BOOLEAN) as {{ column.name }}
        {% else %} CAST(NULL as {{ column.dtype }}) as {{ column.name }}
        {% endif -%}{%- if not loop.last %},{% endif -%}
    {% endfor %}

    --Additionally generating ghost records for the prejoined attributes
    {% if prejoined_columns is not none -%},

      {%- for col, vals in prejoined_columns.items() %}
        {%- set pj_relation_columns = adapter.get_columns_in_relation( source(vals['src_schema']|string, vals['src_table']) ) -%}
        
        {%- for column in pj_relation_columns -%}
          {%- if column.name|lower == vals['bk']|lower -%}
            {% if column.dtype == 'TIMESTAMP' %} {{ dbtvault_scalefree.string_to_timestamp(timestamp_format ,  end_of_all_times) }} as {{ col }}
            {% elif column.dtype == 'VARCHAR' %} '(error)' as {{ col }}
            {% elif column.dtype == 'DECIMAL' %} CAST('-1' as INT64) as {{ col }}
            {% elif column.dtype == 'DOUBLE PRECISION' %} CAST('-1' as FLOAT64) as {{ col }}
            {% elif column.dtype == 'BOOLEAN' %} CAST('FALSE' as BOOLEAN) as {{ col }}
            {% else %} CAST(NULL as {{ column.dtype }}) as {{ col }}
            {% endif -%}{%- if not loop.last %},{% endif -%}
          {%- endif -%}
        {% endfor -%}

      {% endfor -%}

    {%- endif %}
    ),

-- Adding hash unknown values to ghost record
unknown_values_and_hashes AS (
    SELECT

    {{ dbtvault_scalefree.string_to_timestamp(timestamp_format ,  beginning_of_all_times) }} as ldts, 
    'SYSTEM' as rsrc,
    
    unknown_values.*,

    {%- for hash_column in processed_hash_columns %}
    '{{ unknown_key }}' as {{ hash_column }}{{ "," if not loop.last }}
        
    {%- endfor %}

    FROM unknown_values
),

-- Adding hash error values to ghost record
error_values_and_hashes AS (
    SELECT 

    {{ dbtvault_scalefree.string_to_timestamp(timestamp_format ,  end_of_all_times) }} as ldts,
    'ERROR' as rsrc,
    
    error_values.*,

    {%- for hash_column in processed_hash_columns %}
    '{{ error_key }}' as {{ hash_column }}{{ "," if not loop.last }}
        
    {%- endfor %}

    FROM error_values
),

-- Combining all previous ghost record calculations to two rows with the same width as regular entries
ghost_records AS (
    SELECT * FROM unknown_values_and_hashes
    UNION ALL
    SELECT * FROM error_values_and_hashes
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

