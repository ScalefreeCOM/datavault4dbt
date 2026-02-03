{% macro get_rename_column_sql(relation, old_col_name, new_col_name) %}
{{ adapter.dispatch('get_rename_column_sql', 'datavault4dbt')(relation=relation, old_col_name=old_col_name, new_col_name=new_col_name) }}
{% endmacro %}

{% macro default__get_rename_column_sql(relation, old_col_name, new_col_name) %}

    {% if relation.is_dynamic_table -%}
        {% set relation_type = "dynamic table" %}
    {% else -%}
        {% set relation_type = "Table" %}
    {% endif %}

    {% set query %}
    alter {{ relation.get_ddl_prefix_for_alter() }} {{ relation_type }} {{ relation.render() }} rename column {{ old_col_name }} to {{ new_col_name }};
    {% endset %}

    {{ return(query) }}

{% endmacro %}

{% macro databricks__get_rename_column_sql(relation, old_col_name, new_col_name) %}

    {% set query %}
    ALTER TABLE {{ relation.render() }} RENAME COLUMN {{ old_col_name }} TO {{ new_col_name }};
    {% endset %}

    {{ return(query) }}

{% endmacro %}

{% macro snowflake__get_rename_column_sql(relation, old_col_name, new_col_name) %}

    {% if relation.is_dynamic_table -%}
        {% set relation_type = "dynamic table" %}
    {% else -%}
        {% set relation_type = "Table" %}
    {% endif %}

    {% set query %}
    alter {{ relation.get_ddl_prefix_for_alter() }} {{ relation_type }} {{ relation.render() }} rename column {{ old_col_name }} to {{ new_col_name }};
    {% endset %}

    {{ return(query) }}

{% endmacro %}

{% macro synapse__get_rename_column_sql(relation, old_col_name, new_col_name) %}

    {% set query %}
    EXECUTE sp_rename '{{ relation.render() }}.{{ old_col_name }}' , '{{ new_col_name }}' , 'COLUMN';
    {% endset %}

    {{ return(query) }}

{% endmacro %}

{% macro fabric__get_rename_column_sql(relation, old_col_name, new_col_name) %}

    {% set query %}
    EXECUTE sp_rename '{{ relation.render() }}.{{ old_col_name }}' , '{{ new_col_name }}' , 'COLUMN';
    {% endset %}

    {{ return(query) }}

{% endmacro %}

{% macro redshift__get_rename_column_sql(relation, old_col_name, new_col_name) %}

    {% set query %}
    alter table {{ relation.render() }} rename column {{ old_col_name }} to {{ new_col_name }};
    {% endset %}

    {{ return(query) }}

{% endmacro %}

{% macro postgres__get_rename_column_sql(relation, old_col_name, new_col_name) %}

    {% set query %}
    alter table {{ relation.render() }} rename column {{ old_col_name }} to {{ new_col_name }};
    {% endset %}

    {{ return(query) }}

{% endmacro %}

{% macro exasol__get_rename_column_sql(relation, old_col_name, new_col_name) %}

    {% set query %}
        ALTER TABLE {{ relation.render() }} RENAME COLUMN {{ old_col_name }} TO {{ new_col_name }};
    {% endset %}

    {{ log(query, false) }}

    {{ return(query) }}

{% endmacro %}