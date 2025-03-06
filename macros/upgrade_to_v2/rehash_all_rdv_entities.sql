{% macro rehash_all_rdv_entities(entity_yaml, overwrite_hash_values=none) %}

    {# {% do entity_yaml.config.update({'overwrite_hash_values': false}) %} #}

    {% set hub_cols_to_drop =           datavault4dbt.rehash_hubs(entity_yaml, drop_old_values=false) %}
    {% set link_cols_to_drop =          datavault4dbt.rehash_links(entity_yaml, drop_old_values=false) %}
    {% set satellite_cols_to_drop =     datavault4dbt.rehash_satellites(entity_yaml, drop_old_values=false) %}
    {% set ma_satellite_cols_to_drop =  datavault4dbt.rehash_ma_satellites(entity_yaml, drop_old_values=false) %}
    {% set nh_satellite_cols_to_drop =  datavault4dbt.rehash_nh_satellites(entity_yaml, drop_old_values=false) %}

    {% set all_cols_to_drop = hub_cols_to_drop + link_cols_to_drop + satellite_cols_to_drop + ma_satellite_cols_to_drop + nh_satellite_cols_to_drop %}

    {{ log('all_cols_to_drop: ' ~ all_cols_to_drop, false) }}

    {% if overwrite_hash_values == true %}
        {% for model in all_cols_to_drop %}

            {% set model_relation = ref(model.model_name) %}
            {% set cols_to_drop = model.columns_to_drop %}
        
            {{ alter_relation_add_remove_columns(relation=model_relation, remove_columns=cols_to_drop) }}
            {{ log('Old Hash Columns dropped for ' ~ model.model_name, true) }}

        {% endfor %}
    {% endif %}

{% endmacro %}