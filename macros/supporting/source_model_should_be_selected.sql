{% macro source_model_should_be_selected(source_model_name) -%}

    {% set ns = namespace(selected_models=[]) %}
    
    {% set source_model_name = source_model_name.replace('.','_')%}
    {% for item in selected_resources %}
        {% set model_name = item.split('.')[2:] | join('_') %}

        {% set ns.selected_models = ns.selected_models + [model_name] %}
    {% endfor %}

    {{ log('selected_resources: ' ~ selected_resources, false) }}
    {% set model_is_selected = (source_model_name in ns.selected_models) %}
    {{ return(model_is_selected) }}

{%- endmacro -%}