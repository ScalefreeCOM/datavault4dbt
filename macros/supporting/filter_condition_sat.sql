{% macro filter_latest_entries_in_sat(parent_hashkey, src_ldts) -%}
    {{ return(adapter.dispatch('filter_latest_entries_in_sat', 'datavault4dbt')(parent_hashkey = parent_hashkey,
                                                                         src_ldts = src_ldts
    )) }}
{%- endmacro %}

{% macro default__filter_latest_entries_in_sat(parent_hashkey, src_ldts) %}

{% endmacro %}

{% macro databricks__filter_latest_entries_in_sat(parent_hashkey, src_ldts) %}

{% endmacro %}

{% macro exasol__filter_latest_entries_in_sat(parent_hashkey, src_ldts) %}

{% endmacro %}

{% macro fabric__filter_latest_entries_in_sat(parent_hashkey, src_ldts) %}

{% endmacro %}

{% macro oracle__filter_latest_entries_in_sat(parent_hashkey, src_ldts) %}

{% endmacro %}

{% macro postgres__filter_latest_entries_in_sat(parent_hashkey, src_ldts) %}

{% endmacro %}

{% macro redshift__filter_latest_entries_in_sat(parent_hashkey, src_ldts) %}
    WHERE {{ parent_hashkey }} IN (SELECT {{ parent_hashkey }} FROM source_data)
{% endmacro %}

{% macro snowflake__filter_latest_entries_in_sat(parent_hashkey, src_ldts) %}

{% endmacro %}

{% macro synapse__filter_latest_entries_in_sat(parent_hashkey, src_ldts) %}

{% endmacro %}



