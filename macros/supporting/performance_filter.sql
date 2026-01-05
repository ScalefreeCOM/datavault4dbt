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



{% macro filter_distinct_target_hashkey_in_link() -%}
    {{ return(adapter.dispatch('filter_distinct_target_hashkey_in_link', 'datavault4dbt')()) }}
{%- endmacro %}

{% macro default__filter_distinct_target_hashkey_in_link() %}

{% endmacro %}

{% macro databricks__filter_distinct_target_hashkey_in_link() %}

{% endmacro %}

{% macro exasol__filter_distinct_target_hashkey_in_link() %}

{% endmacro %}

{% macro fabric__filter_distinct_target_hashkey_in_link() %}

{% endmacro %}

{% macro oracle__filter_distinct_target_hashkey_in_link() %}

{% endmacro %}

{% macro postgres__filter_distinct_target_hashkey_in_link() %}

{% endmacro %}

{% macro redshift__filter_distinct_target_hashkey_in_link() %}

{% endmacro %}

{% macro snowflake__filter_distinct_target_hashkey_in_link() %}

{% endmacro %}

{% macro synapse__filter_distinct_target_hashkey_in_link() %}

{% endmacro %}



{% macro filter_distinct_target_hashkey_in_nh_link() -%}
    {{ return(adapter.dispatch('filter_distinct_target_hashkey_in_nh_link', 'datavault4dbt')()) }}
{%- endmacro %}

{% macro default__filter_distinct_target_hashkey_in_nh_link() %}

{% endmacro %}

{% macro databricks__filter_distinct_target_hashkey_in_nh_link() %}

{% endmacro %}

{% macro exasol__filter_distinct_target_hashkey_in_nh_link() %}

{% endmacro %}

{% macro fabric__filter_distinct_target_hashkey_in_nh_link() %}

{% endmacro %}

{% macro oracle__filter_distinct_target_hashkey_in_nh_link() %}

{% endmacro %}

{% macro postgres__filter_distinct_target_hashkey_in_nh_link() %}

{% endmacro %}

{% macro redshift__filter_distinct_target_hashkey_in_nh_link() %}

{% endmacro %}

{% macro snowflake__filter_distinct_target_hashkey_in_nh_link() %}

{% endmacro %}

{% macro synapse__filter_distinct_target_hashkey_in_nh_link() %}

{% endmacro %}



{% macro filter_distinct_target_hashkey_in_hub() -%}
    {{ return(adapter.dispatch('filter_distinct_target_hashkey_in_hub', 'datavault4dbt')()) }}
{%- endmacro %}

{% macro default__filter_distinct_target_hashkey_in_hub() %}

{% endmacro %}

{% macro databricks__filter_distinct_target_hashkey_in_hub() %}

{% endmacro %}

{% macro exasol__filter_distinct_target_hashkey_in_hub() %}

{% endmacro %}

{% macro fabric__filter_distinct_target_hashkey_in_hub() %}

{% endmacro %}

{% macro oracle__filter_distinct_target_hashkey_in_hub() %}

{% endmacro %}

{% macro postgres__filter_distinct_target_hashkey_in_hub() %}

{% endmacro %}

{% macro redshift__filter_distinct_target_hashkey_in_hub() %}

{% endmacro %}

{% macro snowflake__filter_distinct_target_hashkey_in_hub() %}

{% endmacro %}

{% macro synapse__filter_distinct_target_hashkey_in_hub() %}

{% endmacro %}