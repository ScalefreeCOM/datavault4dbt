{% macro yaml_metadata_parser(name=none, yaml_metadata=none, parameter=none, required=False, documentation=none) %}

    {% if datavault4dbt.is_something(yaml_metadata) %}
        {%- set metadata_dict = fromyaml(yaml_metadata) -%}
        {% if name in metadata_dict.keys() %}
            {% set return_value = metadata_dict.get(name) %}
            {% if datavault4dbt.is_something_or_false(parameter)%}
                {{ log("[" ~ this ~ "] Parameter '" ~ name ~ "' defined both in yaml-metadata and separately. Value from yaml-metadata will be used, and separate parameter is ignored.", info=False) }}
            {% endif %}
        {% elif datavault4dbt.is_something_or_false(parameter) %}
            {% set return_value = parameter %}
            {{ log("[" ~ this ~ "] yaml-metadata given, but parameter '" ~ name ~ "' not defined in there. Applying '" ~ parameter ~ "' which is either a parameter passed separately or the default value.", info=False) }}
        {% elif required %}
            {{ exceptions.raise_compiler_error("[" ~ this ~ "] Error: yaml-metadata given, but required parameter '" ~ name ~ "' not defined in there or outside in the parameter. \n Description of parameter '" ~ name ~ "': \n" ~ documentation ) }}
        {% else %}
            {% set return_value = None %}
        {% endif %}
    {% elif datavault4dbt.is_something_or_false(parameter) %}
        {% set return_value = parameter %}
    {% elif required %}
        {{ exceptions.raise_compiler_error("[" ~ this ~ "] Error: Required parameter '" ~ name ~ "' not defined. Define it either directly, or inside yaml-metadata. \n Description of parameter '" ~ name ~ "': \n" ~ documentation ) }}
    {% else %}
        {% set return_value = None %}
    {% endif %}

    {{ return(return_value) }}

{% endmacro %}
