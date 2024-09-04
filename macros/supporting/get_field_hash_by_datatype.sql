{# This macro compares all hashed_columns with all columns of object "Column" to determine the data type of the hashed column and returns the corrected hashed_columns with escaped values #}

{%- macro get_field_hash_by_datatype(hashed_columns, all_datatype_columns, derived_columns=none) -%}
  {%- set tmp_columns_of_hashed_column = [] -%}
  {%- if datavault4dbt.is_list(hashed_columns) -%}
    {%- for hash_column in hashed_columns -%}
      {%- set ns_hash_column_new = namespace(hash_column_new=hash_column) -%}
      {%- for column in all_datatype_columns -%}
        {%- if hash_column|lower == column.name|lower -%}
          {%- if derived_columns[hash_column] and derived_columns[hash_column] is mapping and derived_columns[hash_column]['datatype']-%}
            {%- set datatype = derived_columns[hash_column]['datatype'] | string | upper | trim -%}
          {%- else -%}
            {%- set datatype = column.data_type | string | upper | trim -%}
          {%- endif -%}
          {%- if datatype == 'BOOLEAN' -%}
            {%- set ns_hash_column_new.hash_column_new = 'DECODE('~hash_column~', true, 1, false, 0)' -%}
          {%- elif datatype == 'GEOMETRY' -%}
            {%- set ns_hash_column_new.hash_column_new = 'FNV_HASH(ST_AsBinary('~hash_column~'))' -%}
          {%- elif datatype == 'SUPER' -%}
            {%- set ns_hash_column_new.hash_column_new = 'JSON_SERIALIZE('~hash_column~')' -%}
          {%- endif -%}

        {%- break -%}
        {%- endif -%}
      {%- endfor -%}
      {%- do tmp_columns_of_hashed_column.append(ns_hash_column_new.hash_column_new) -%}
    {%- endfor -%}
  {%- endif -%}
  {{ return(tmp_columns_of_hashed_column) }}
{%- endmacro -%}