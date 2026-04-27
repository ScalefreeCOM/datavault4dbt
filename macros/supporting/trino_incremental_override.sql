{# ============================================================
   Trino incremental materialization override for datavault4dbt
   ============================================================
   Replaces the dbt-trino built-in incremental materialization
   to work around two Trino memory connector limitations:

   1. expand_target_column_types: the built-in trino__alter_column_type
      uses UPDATE which the memory connector does not support.
      We replace it with CTAS + DROP + RENAME (see trino_expand_column_types.sql).

   2. dest_columns intersection: when on_schema_change='ignore', the
      fallback uses ALL existing table columns (including _new columns
      added by no-overwrite rehash). If the model does not produce
      those columns, Trino raises COLUMN_NOT_FOUND.
      We restrict the INSERT to the intersection of source and target.
#}

{% materialization incremental, adapter='trino', supported_languages=['sql'] -%}

  {#-- configs --#}
  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set language = model['language'] -%}
  {%- set on_table_exists = config.get('on_table_exists', 'rename') -%}
  {% if on_table_exists not in ['rename', 'drop', 'replace'] %}
      {%- set log_message = 'Invalid value for on_table_exists (%s) specified. Setting default value (%s).' % (on_table_exists, 'rename') -%}
      {% do log(log_message) %}
      {%- set on_table_exists = 'rename' -%}
  {% endif %}
  {#-- Get the incremental_strategy and the macro to use for the strategy --#}
  {% set incremental_strategy = config.get('incremental_strategy') or 'default' %}
  {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
  {% set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(context, incremental_strategy) %}

  {#-- relations --#}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {#-- The temp relation will be a view (faster) or temp table, depending on upsert/merge strategy --#}
  {%- set tmp_relation_type = get_incremental_tmp_relation_type(incremental_strategy, unique_key, language) -%}
  {%- set tmp_relation = make_temp_relation(this).incorporate(type=tmp_relation_type) -%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}

  {%- set preexisting_tmp_relation = load_cached_relation(tmp_relation)-%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation)-%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}

  {% set grant_config = config.get('grants') %}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_tmp_relation) }}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none %}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}

  {% elif existing_relation.is_view %}
    {{ log("Dropping relation " ~ target_relation ~ " because it is a view and this model is a table.") }}
    {% do adapter.drop_relation(existing_relation) %}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}
  {% elif full_refresh_mode %}
    {% do on_table_exists_logic(on_table_exists, existing_relation, intermediate_relation, backup_relation, target_relation) %}

  {% else %}
    {#-- Create the temp relation, either as a view or as a temp table --#}
    {% if tmp_relation_type == 'view' %}
        {%- call statement('create_tmp_relation') -%}
          {{ create_view_as(tmp_relation, compiled_code) }}
        {%- endcall -%}
    {% else %}
        {%- call statement('create_tmp_relation', language=language) -%}
          {{ create_table_as(True, tmp_relation, compiled_code, language) }}
        {%- endcall -%}
    {% endif %}

    {#-- Use CTAS-based column expansion instead of adapter.expand_target_column_types
         to avoid the built-in trino__alter_column_type which uses UPDATE (unsupported
         by the Trino memory connector). --#}
    {{ datavault4dbt.trino__expand_column_types(tmp_relation, target_relation) }}

    {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
    {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
    {% if not dest_columns %}
      {#-- With on_schema_change='ignore', restrict INSERT to columns that exist in BOTH
           source and target. This prevents COLUMN_NOT_FOUND errors when the target table
           has extra columns (e.g. _new columns added by no-overwrite rehash) that the
           model does not produce. --#}
      {%- set src_col_names = adapter.get_columns_in_relation(tmp_relation)
                                | map(attribute='name') | map('lower') | list -%}
      {%- set dest_columns = [] -%}
      {%- for col in adapter.get_columns_in_relation(existing_relation) -%}
        {%- if col.name.lower() in src_col_names -%}
          {%- do dest_columns.append(col) -%}
        {%- endif -%}
      {%- endfor -%}
    {% endif %}

    {#-- Build the sql --#}
    {% set strategy_arg_dict = ({'target_relation': target_relation, 'temp_relation': tmp_relation, 'unique_key': unique_key, 'dest_columns': dest_columns, 'incremental_predicates': incremental_predicates }) %}
    {%- call statement('main') -%}
      {{ strategy_sql_macro_func(strategy_arg_dict) }}
    {%- endcall -%}
  {% endif %}
    {% do drop_relation_if_exists(tmp_relation) %}
  {{ run_hooks(post_hooks) }}

  {% set should_revoke =
   should_revoke(existing_relation.is_table, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
