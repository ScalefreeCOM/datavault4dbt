{#
    This macro creates primary key constraints
    Parameters:
    name::string                        Name of the constraint
    columns::list                       List of columns, which resemble the primary key
    tabletype::string                   Type of data vault table such as hub, link, satellite.
    ldts::string                        Name of the column inside the source data, that holds information about the Load Date Timestamp. Can also be a SQL expression.
#}
{%- macro oracle__primary_key(name, columns, tabletype=none) -%}


    {# Add load date as primary key column for satellites #}
    {%- set ldts_alias = var('datavault4dbt.ldts_alias', 'ldts') -%}
    {% if tabletype=='satellite' %}
      {% do columns.append(ldts_alias) %}
    {% endif %}

    {% if execute %}
      {# Drop exisiting constraints #}
      {% set backup_relation = api.Relation.create(schema=this.schema, identifier=this.identifier~"__dbt_backup", type='table') %}
      {%- if dbt_constraints.unique_constraint_exists(table_relation=backup_relation, column_names=columns, lookup_cache=none) -%}
        {%- do log("Dropping constraints of table: "~backup_relation.identifier, info=false) -%}
        {{ dbt_constraints.oracle__drop_referential_constraints(relation=backup_relation) }}
      {% endif %}

      {# Create Constraint #}
      {% set new_relation = api.Relation.create(schema=this.schema, identifier=this.identifier, type='table') %}
      {{ dbt_constraints.create_primary_key(table_model=new_relation, column_names=columns, verify_permissions=false, quote_columns=false, constraint_name=name, lookup_cache=none) }}
    {% endif %}

{%- endmacro -%}