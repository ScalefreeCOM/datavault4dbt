{% macro source_model_should_be_selected(source_model_name) -%}

    {{ log('contect_project_name: ' ~ context['project_name'], true) }}

    {% set model_id = 'model.' + context['project_name'] + '.' + source_model_name %}

    {{ log('model_id: ' ~ model_id, true) }}
    {{ log('selected_resources: ' ~ selected_resources, true) }}
    {% set model_is_selected = (model_id in selected_resources) %}

    {{ return(model_is_selected) }}

{%- endmacro -%}