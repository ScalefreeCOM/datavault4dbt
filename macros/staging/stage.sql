{%- macro stage(ldts,
                rsrc, 
                include_source_columns, 
                source_schema,
                source_table, 
                hashed_columns=none, 
                derived_columns=none, 
                ranked_columns=none, 
                sequence=none,
                prejoined_columns=none,
                missing_columns=none) -%}

{%- set derived_column_names = dbtvault.extract_column_names(derived_columns) -%}
{%- set hashed_column_names = dbtvault.extract_column_names(hashed_columns) -%}
{%- set ranked_column_names = dbtvault.extract_column_names(ranked_columns) -%}
{%- set exclude_column_names = derived_column_names + hashed_column_names %}

WITH

-- Selecting all columns from the source data, renaming load date and record source to Scalefree naming conventions
source_data AS (
  SELECT
    {{ 'src.'+ldts }} AS ldts
    ,src.{{ rsrc }} AS rsrc
    {{ ',src.{{ sequence }} as edwSequence' if sequence is not none }}
    ,src.*

  FROM {{ source(source_schema|string, source_table ) }} as src

  {%- set last_cte = "source_data" -%}
)

{%- if missing_columns is not none -%},

-- Filling missing columns with NULL values for schema changes
missing_columns AS (

  SELECT 

  *,

  {%- for col, dtype in missing_columns.items() %}
    ,CAST(NULL as {{ dtype }}) as {{ col }}
  {% endfor %}

  FROM {{ last_cte }}
  {%- set last_cte = "missing_columns" -%}
)
{%- endif -%}

-- Prejoining Business Keys of other source objects for Link purposes
{%- if prejoined_columns is not none -%},

prejoined_columns AS (  
  
  SELECT

  lcte.*

  {%- for col, vals in prejoined_columns.items() %}
    ,pj_{{loop.index}}.{{ col }}
  {% endfor -%}

  FROM {{ last_cte }} lcte

  {%- for col, vals in prejoined_columns.items() %}
    left join {{ source(vals['src_schema']|string, vals['src_table']) }} as pj_{{loop.index}} on lcte.{{ vals['this_column_name'] }} = pj_{{loop.index}}.{{ vals['ref_column_name'] }}
  {% endfor %}

  {% set last_cte = "prejoined_columns" -%}
)
{%- endif -%}

-- Adding derived columns to the selection
{%- if derived_columns is not none -%},

derived_columns AS (

    SELECT

    *,
    {{ dbtvault.derive_columns(columns=derived_columns) | indent(4) }}

    FROM {{ last_cte }}
    {%- set last_cte = "derived_columns" -%}
)
{%- endif -%}

-- Generating Hashed Columns (hashkeys and hashdiffs for Hubs/Links/Satellites)
{% if dbtvault.is_something(hashed_columns) -%},

hashed_columns AS (

    SELECT

    *,

    {% set processed_hash_columns = dbtvault.process_hash_column_excludes(hashed_columns) -%}
    {{- hash_columns(columns=processed_hash_columns) | indent(4) }}

    FROM {{ last_cte }}
    {%- set last_cte = "hashed_columns" -%}
)
{%- endif -%}

-- Adding Ranked Columsn to the selection
{% if dbtvault.is_something(ranked_columns) -%},

ranked_columns AS (

    SELECT *,

    {{ dbtvault.rank_columns(columns=ranked_columns) | indent(4) if dbtvault.is_something(ranked_columns) }}

    FROM {{ last_cte }}
    {%- set last_cte = "ranked_columns" -%}
)
{%- endif -%},

-- Creating Ghost Record for unknown case, based on datatype
unknown_values AS (
    {%- set all_columns = adapter.get_columns_in_relation( source(source_schema|string, source_table )) -%}
    {%- set special_columns = ['edwRecordSource', 'rsrc_file'] -%}

    SELECT
    {%- for column in all_columns -%}
        {%- if column.name in special_columns %} 'SYSTEM' as {{ column.name }}
        {% elif column.dtype == 'TIMESTAMP' %} PARSE_TIMESTAMP('%Y-%m-%dT%H-%M-%S', '0001-01-01T00-00-01') as {{ column.name }}
        {% elif column.dtype == 'STRING' %} '(unknown)' as {{ column.name }}
        {% elif column.dtype == 'INT64' %} CAST('0' as INT64) as {{ column.name }}
        {% elif column.dtype == 'FLOAT64' %} CAST('0' as FLOAT64) as {{ column.name }}
        {% elif column.dtype == 'BOOLEAN' %} CAST('FALSE' as BOOLEAN) as {{ column.name }}
        {% else %} CAST(NULL as {{ column.dtype }}) as {{ column.name }}
        {% endif -%}{%- if not loop.last %},{% endif -%}
    {% endfor %}

    --Additionally generating ghost records for the prejoined attributes
    {%- if prejoined_columns is not none -%},
      {%- for col, vals in prejoined_columns.items() %}
        {%- set pj_relation_columns = adapter.get_columns_in_relation( source(vals['src_schema']|string, vals['src_table']) ) -%}
        
          {%- for column in pj_relation_columns -%}
            {%- if column.name|lower == col|lower -%}
              {% if column.dtype == 'TIMESTAMP' %} PARSE_TIMESTAMP('%Y-%m-%dT%H-%M-%S', '0001-01-01T00-00-01') as {{ column.name }}
              {% elif column.dtype == 'STRING' %} '(unknown)' as {{ column.name }}
              {% elif column.dtype == 'INT64' %} CAST('0' as INT64) as {{ column.name }}
              {% elif column.dtype == 'FLOAT64' %} CAST('0' as FLOAT64) as {{ column.name }}
              {% elif column.dtype == 'BOOLEAN' %} CAST('FALSE' as BOOLEAN) as {{ column.name }}
              {% else %} CAST(NULL as {{ column.dtype }}) as {{ column.name }}
              {% endif -%}{%- if not loop.last %},{% endif -%}
            {%- endif -%}
          {% endfor -%}
        
        {% endfor -%}

    {%- endif -%}

    --FROM {{ this.database }}.{{ source_schema }}.{{ source_table }} LIMIT 1 
    ),

--Creating Ghost Record for error case, based on datatype
error_values AS (
    {%- set all_columns = adapter.get_columns_in_relation(source(source_schema|string, source_table)) -%}
    {%- set special_columns = [rsrc] -%}

    SELECT
    {%- for column in all_columns -%}
        {%- if column.name in special_columns %} 'ERROR' as {{ column.name }}
        {% elif column.dtype == 'TIMESTAMP' %} PARSE_TIMESTAMP('%Y-%m-%dT%H-%M-%S', '8888-12-31T23-59-59') as {{ column.name }}
        {% elif column.dtype == 'STRING' %} '(error)' as {{ column.name }}
        {% elif column.dtype == 'INT64' %} CAST('-1' as INT64) as {{ column.name }}
        {% elif column.dtype == 'FLOAT64' %} CAST('-1' as FLOAT64) as {{ column.name }}
        {% elif column.dtype == 'BOOLEAN' %} CAST('FALSE' as BOOLEAN) as {{ column.name }}
        {% else %} CAST(NULL as {{ column.dtype }}) as {{ column.name }}
        {% endif -%}{%- if not loop.last %},{% endif -%}
    {% endfor %}

    --Additionally generating ghost records for the prejoined attributes
    {%- if prejoined_columns is not none -%},

      {%- for col, vals in prejoined_columns.items() %}
        {%- set pj_relation_columns = adapter.get_columns_in_relation( source(vals['src_schema']|string, vals['src_table']) ) -%}
        
        {%- for column in pj_relation_columns -%}
          {%- if column.name|lower == col|lower -%}
            {% if column.dtype == 'TIMESTAMP' %} PARSE_TIMESTAMP('%Y-%m-%dT%H-%M-%S', '8888-12-31T23-59-59') as {{ column.name }}
            {% elif column.dtype == 'STRING' %} '(error)' as {{ column.name }}
            {% elif column.dtype == 'INT64' %} CAST('-1' as INT64) as {{ column.name }}
            {% elif column.dtype == 'FLOAT64' %} CAST('-1' as FLOAT64) as {{ column.name }}
            {% elif column.dtype == 'BOOLEAN' %} CAST('FALSE' as BOOLEAN) as {{ column.name }}
            {% else %} CAST(NULL as {{ column.dtype }}) as {{ column.name }}
            {% endif -%}{%- if not loop.last %},{% endif -%}
          {%- endif -%}
        {% endfor -%}

      {% endfor -%}

    {%- endif -%}

    FROM {{ this.database }}.{{ source_schema }}.{{ source_table }} LIMIT 1 ),

-- Adding hash unknown values to ghost record
unknown_values_and_hashes AS (
    SELECT

    PARSE_TIMESTAMP('%Y-%m-%dT%H-%M-%S', '0001-01-01T00-00-01') as ldts, 
    'SYSTEM' as rsrc,
    
    *,

    {%- for hash_column in processed_hash_columns %}
    '00000000000000000000000000000000' as {{ hash_column }}{{ "," if not loop.last }}
        
    {%- endfor %}

    FROM unknown_values
),

-- Adding hash error values to ghost record
error_values_and_hashes AS (
    SELECT 

    PARSE_TIMESTAMP('%Y-%m-%dT%H-%M-%S', '8888-12-31T23-59-59') as ldts,
    'ERROR' as rsrc,
    
    *,

    {%- for hash_column in processed_hash_columns %}
    'ffffffffffffffffffffffffffffffff' as {{ hash_column }}{{ "," if not loop.last }}
        
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