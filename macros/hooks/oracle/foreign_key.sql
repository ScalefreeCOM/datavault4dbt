{#
    This macro creates primary key constraints
    Parameters:
    name                                        Name of the constraint
    pk_table_relation::string                   Name of the table that holds the primary key
    pk_column_names::list                       List of columns, which resemble the primary key
    fk_table_relation::string                   Name of the table that holds the foreign key constrain
    fk_column_names::list                       List of columns, which resemble the foreign key

#}
{%- macro oracle__foreign_key(name, pk_table_relation, pk_column_names, fk_table_relation, fk_column_names) -%}

    {% if execute %}
      {# Drop exisiting constraints #}
      {% set backup_relation = api.Relation.create(schema=this.schema, identifier=this.identifier~"__dbt_backup", type='table') %}
      {%- if dbt_constraints.unique_constraint_exists(table_relation=backup_relation, column_names=columns, lookup_cache=none) -%}
        {%- do log("Dropping constraints of table: "~backup_relation.identifier, info=false) -%}
        {{ dbt_constraints.oracle__drop_referential_constraints(relation=backup_relation) }}
      {% endif %}

      {# Create Constraint #}
      {% set tmp_pk_table_relation = api.Relation.create(schema=this.schema, identifier=pk_table_relation, type='table') %}
      {% set tmp_fk_table_relation = api.Relation.create(schema=this.schema, identifier=this.identifier, type='table') %}

      {{ dbt_constraints.oracle__create_foreign_key(pk_table_relation=tmp_pk_table_relation,
                                                    pk_column_names=pk_column_names,
                                                    fk_table_relation=tmp_fk_table_relation,
                                                    fk_column_names=fk_column_names,
                                                    verify_permissions=false,
                                                    quote_columns=false,
                                                    constraint_name=name,
                                                    lookup_cache=none) }}
    {% endif %}

{%- endmacro -%}