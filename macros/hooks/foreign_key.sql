{#
    This macro creates primary key constraints
    Parameters:
    name                                        Name of the constraint
    pk_table_relation::string                   Name of the table that holds the primary key
    pk_column_names::list                       List of columns, which resemble the primary key
    fk_table_relation::string                   Name of the table that holds the foreign key constrain
    fk_column_names::list                       List of columns, which resemble the foreign key

#}
{%- macro foreign_key(name, pk_table_relation, pk_column_names, fk_table_relation, fk_column_names) -%}



    {{ return(adapter.dispatch('foreign_key', 'datavault4dbt')(name=name,
                                                               pk_table_relation=pk_table_relation,
                                                               pk_column_names=pk_column_names,
                                                               fk_table_relation=fk_table_relation,
                                                               fk_column_names=fk_column_names)) }}
{%- endmacro -%}