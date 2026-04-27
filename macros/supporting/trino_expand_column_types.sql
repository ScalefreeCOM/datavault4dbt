{% macro trino__expand_column_types(from_relation, to_relation) %}
  {# Replacement for adapter.expand_target_column_types on the Trino memory connector.
     For each column in from_relation that is wider than the matching column in
     to_relation, widens the to_relation column using CTAS + DROP + RENAME.
     The built-in trino__alter_column_type uses UPDATE which the memory connector
     does not support. #}
  {%- if execute -%}
    {%- set src_cols = adapter.get_columns_in_relation(from_relation) -%}
    {%- set tgt_cols_dict = {} -%}
    {%- for col in adapter.get_columns_in_relation(to_relation) -%}
      {%- do tgt_cols_dict.update({col.name.lower(): col}) -%}
    {%- endfor -%}

    {%- for src_col in src_cols -%}
      {%- set tgt_col = tgt_cols_dict.get(src_col.name.lower()) -%}
      {%- if tgt_col is not none and tgt_col.can_expand_to(src_col) -%}
        {%- set col_size = src_col.string_size() -%}
        {%- set new_type = api.Column.string_type(col_size) -%}
        {{ log('Expanding column ' ~ src_col.name ~ ' on ' ~ to_relation ~ ' from ' ~ tgt_col.data_type ~ ' to ' ~ new_type, false) }}

        {%- set tmp_id = to_relation.identifier ~ '__expand_tmp' -%}
        {%- set tmp_rel = api.Relation.create(
            database=to_relation.database,
            schema=to_relation.schema,
            identifier=tmp_id
        ) -%}

        {%- set all_tgt_cols = adapter.get_columns_in_relation(to_relation) -%}
        {%- set col_select_list = [] -%}
        {%- for col in all_tgt_cols -%}
          {%- if col.name.lower() == src_col.name.lower() -%}
            {%- do col_select_list.append(
              'CAST(' ~ adapter.quote(col.name) ~ ' AS ' ~ new_type ~ ') AS ' ~ adapter.quote(col.name)
            ) -%}
          {%- else -%}
            {%- do col_select_list.append(adapter.quote(col.name)) -%}
          {%- endif -%}
        {%- endfor -%}

        {% do run_query('DROP TABLE IF EXISTS ' ~ tmp_rel) %}
        {% set ctas_sql %}
          CREATE TABLE {{ tmp_rel }} AS
          SELECT {{ col_select_list | join(', ') }}
          FROM {{ to_relation }}
        {% endset %}
        {% do run_query(ctas_sql) %}
        {% do run_query('DROP TABLE ' ~ to_relation) %}
        {% do run_query('ALTER TABLE ' ~ tmp_rel ~ ' RENAME TO ' ~ to_relation.identifier) %}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
{% endmacro %}
