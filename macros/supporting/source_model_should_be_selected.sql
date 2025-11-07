{% macro source_model_should_be_selected(source_model_name) -%}

    {{ log('contect_project_name: ' ~ context['project_name'], false) }}

    {% set model_id = 'model.' + context['project_name'] + '.' + source_model_name %}

    {{ log('model_id: ' ~ model_id, false) }}
    {{ log('selected_resources: ' ~ selected_resources, false) }}
    {% set model_is_selected = (model_id in selected_resources) %}

    {{ return(model_is_selected) }}

{%- endmacro -%}