{#
    Works for standard links and non-historized links on Oracle.

    Example usage: 
    dbt run-operation rehash_single_link --args '{link: customer_nation_l, link_hashkey: hk_customer_nation_l, overwrite_hash_values: true, hub_config: [{hub_hashkey: hk_customer_h, hub_name: customer_h, business_keys: [c_custkey]}, {hub_hashkey: hk_nation_h, hub_name: nation_h, business_keys: [n_nationkey]}]}'
#}

{% macro oracle__rehash_single_link(link, link_hashkey, hub_config, additional_hash_input_cols=[], overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set new_link_hashkey_name = link_hashkey ~ '_new' %}
    
    {# Oracle default for MD5 is usually VARCHAR2(32) #}
    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'VARCHAR2(32)') %}
    
    {% set ns = namespace(hub_hashkeys=[], new_hash_columns=[{"name": new_link_hashkey_name, "data_type": hash_datatype}], columns_to_drop=[{"name": link_hashkey + '_deprecated'}]) %}

    {% set link_relation = ref(link) %}

    {# 1. Prepare configuration for Hubs #}
    {% for hub in hub_config %}
        {% set hub_join_alias = 'hub' ~ loop.index %}
        {% set prefixed_business_keys = datavault4dbt.prefix(columns=hub.business_keys, prefix_str=hub_join_alias).split(',') %}
        {% set new_hub_hashkey_name =  hub.hub_hashkey ~ '_new' %}

        {% set hub_hashkey_dict = {
            "current_hashkey_name": hub.hub_hashkey,
            "new_hashkey_name": new_hub_hashkey_name,
            "hub_name": hub.hub_name,
            "hub_join_alias": hub_join_alias,
            "prefixed_business_keys": prefixed_business_keys
        } %}

        {% do ns.hub_hashkeys.append(hub_hashkey_dict) %}

        {% set new_hash_col_dict = {
            "name": new_hub_hashkey_name,
            "data_type": hash_datatype
        } %}

        {% do ns.new_hash_columns.append(new_hash_col_dict) %}

    {% endfor %}

    {# 2. Add new columns (new Link Hash & new Hub Hashes) #}
    {{ log('Executing ALTER TABLE statement (Adding new columns)...', output_logs) }}
    {{ datavault4dbt.alter_relation_add_remove_columns(relation=link_relation, add_columns=ns.new_hash_columns) }}
    {{ log('ALTER TABLE statement completed!', output_logs) }}

    {# 3. Generate Update Statement (using Oracle MERGE) #}
    {% set update_sql = datavault4dbt.link_update_statement(link_relation=link_relation,
                                                            hub_hashkeys=ns.hub_hashkeys,
                                                            link_hashkey=link_hashkey,
                                                            new_link_hashkey_name=new_link_hashkey_name,
                                                            additional_hash_input_cols=additional_hash_input_cols) %}

    {# Execute the MERGE Update #}
    {{ log('Executing UPDATE (MERGE) statement...', output_logs) }}
    {{ '/* UPDATE STATEMENT FOR ' ~ link ~ '\n' ~ update_sql ~ '*/' }}
    {% do run_query(update_sql) %}
    {{ log('UPDATE statement completed!', output_logs) }}

    {# 4. Rename columns #}
    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values with new ones...', output_logs) }}

        {# Rename Link Hashkey #}
        {% do run_query("ALTER TABLE " ~ link_relation ~ " RENAME COLUMN " ~ link_hashkey ~ " TO " ~ link_hashkey ~ "_deprecated") %}
        {% do run_query("ALTER TABLE " ~ link_relation ~ " RENAME COLUMN " ~ new_link_hashkey_name ~ " TO " ~ link_hashkey) %}

        {# Rename All Hub Hashkeys #}
        {% for hub_hashkey in ns.hub_hashkeys %}
            {% do run_query("ALTER TABLE " ~ link_relation ~ " RENAME COLUMN " ~ hub_hashkey.current_hashkey_name ~ " TO " ~ hub_hashkey.current_hashkey_name ~ "_deprecated") %}
            {% do run_query("ALTER TABLE " ~ link_relation ~ " RENAME COLUMN " ~ hub_hashkey.new_hashkey_name ~ " TO " ~ hub_hashkey.current_hashkey_name) %}

            {# Add to drop list #}
            {% do ns.columns_to_drop.append({"name": hub_hashkey.current_hashkey_name + '_deprecated'}) %}
        {% endfor %}
        
        {# 5. Drop deprecated columns #}
        {% if drop_old_values %}
            {{ log('Dropping deprecated columns...', output_logs) }}
            {{ datavault4dbt.alter_relation_add_remove_columns(relation=link_relation, remove_columns=ns.columns_to_drop) }}
            {{ log('Existing Hash values overwritten and old columns dropped!', true) }}
        {% endif %}

    {% endif %}

    {{ return(ns.columns_to_drop) }}

{% endmacro %}


{% macro oracle__link_update_statement(link_relation, hub_hashkeys, link_hashkey, new_link_hashkey_name, additional_hash_input_cols) %}

    {% set ns = namespace(link_hashkey_input_cols=[], hash_config_dict={}) %}
    
    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}
    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}

    {# Prepare Hash Config for Hubs #}
    {% for hub_hashkey in hub_hashkeys %}
        {% do ns.hash_config_dict.update({hub_hashkey.new_hashkey_name: hub_hashkey.prefixed_business_keys}) %}
        {% set ns.link_hashkey_input_cols = ns.link_hashkey_input_cols + hub_hashkey.prefixed_business_keys %}
    {% endfor %}

    {# Prepare Hash Config for the Link itself #}
    {% set ns.link_hashkey_input_cols = ns.link_hashkey_input_cols + additional_hash_input_cols %}
    {% do ns.hash_config_dict.update({new_link_hashkey_name: ns.link_hashkey_input_cols}) %}

    {{ log('hash_config: ' ~ ns.hash_config_dict, false)}}

    {# Begin MERGE Statement Construction #}
    {% set merge_sql %}
    MERGE INTO {{ link_relation }} target
    USING (
        SELECT
            link.{{ link_hashkey }} as original_link_key,
            {{ datavault4dbt.hash_columns(columns=ns.hash_config_dict) }}
        FROM {{ link_relation }} link
        
        {% for hub in hub_hashkeys %}
            {% set hub_ns = namespace(hub_already_rehashed=false) %}
            
            {# Check if Hub was already rehashed (deprecated column exists) #}
            {% set all_hub_columns = adapter.get_columns_in_relation(ref(hub.hub_name)) %}
            {% for column in all_hub_columns %}
                {% if column.name|lower == hub.current_hashkey_name|lower + '_deprecated' %}
                    {% set hub_ns.hub_already_rehashed = true %}
                {% endif %}
            {% endfor %}

            {% if hub_ns.hub_already_rehashed %}
                {% set join_hashkey_col = hub.current_hashkey_name + '_deprecated' %}
            {% else %}
                {% set join_hashkey_col = hub.current_hashkey_name %}
            {% endif %}

            LEFT JOIN {{ ref(hub.hub_name).render() }} {{ hub.hub_join_alias }} 
                ON link.{{ hub.current_hashkey_name }} = {{ hub.hub_join_alias }}.{{ join_hashkey_col }}
        {% endfor %}

        WHERE link.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
        
        UNION ALL

        {# Handle Ghost Records / Error Records: Pass-through existing keys #}
        SELECT 
            link.{{ link_hashkey }} as original_link_key
            
            {% for hub in hub_hashkeys %}
            , link.{{ hub.current_hashkey_name }} as {{ hub.new_hashkey_name }}
            {% endfor %}
            , link.{{ link_hashkey }} as {{ new_link_hashkey_name }}
            
        FROM {{ link_relation }} link
        WHERE link.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

    ) src
    ON (target.{{ link_hashkey }} = src.original_link_key)
    
    WHEN MATCHED THEN
        UPDATE SET 
            target.{{ new_link_hashkey_name }} = src.{{ new_link_hashkey_name }}
            {% for hub in hub_hashkeys %}
            , target.{{ hub.new_hashkey_name }} = src.{{ hub.new_hashkey_name }}
            {% endfor %}
    {% endset %}

    {{ return(merge_sql) }}

{% endmacro %}