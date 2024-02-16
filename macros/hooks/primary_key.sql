{#
    This macro creates primary key constraints
    Parameters:
    name::string                        Name of the constraint
    columns::list                       List of columns, which resemble the primary key
    tabletype::string                   Type of data vault table such as hub, link, satellite.
#}
{%- macro primary_key(name, columns, tabletype=none) -%}


    {{ return(adapter.dispatch('primary_key', 'datavault4dbt')(name=name,
                                                               columns=columns,
                                                               tabletype=tabletype)) }}

{%- endmacro -%}