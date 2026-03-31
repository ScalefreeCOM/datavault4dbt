{% macro get_static_analysis_config(macro_name) -%}

    {% set static_analysis_yaml %}
    bigquery:
        - stage
        - control_snap_v0
    databricks: 
        - stage
        - sat_v1
        - ma_sat_v1
        - ref_sat_v1
        - control_snap_v1
    redshift:
        - ma_sat_v1
    {% endset %}

    {%- set static_analysis_dict = fromyaml(static_analysis_yaml) -%}

    {% set use_static_analysis_overwrite = var('datavault4dbt.enable_static_analysis_overwrite', True) %}

    {%- if (use_static_analysis_overwrite == True or use_static_analysis_overwrite == true)  and target.type in static_analysis_dict.keys() -%}
        {%- if macro_name in static_analysis_dict[target.type] -%}
            {%- set static_analysis_config = 'off' -%}
        {%- else -%}
            {%- set static_analysis_config = none -%}
        {%- endif -%}
    {%- else -%}
        {%- set static_analysis_config = none -%}
    {%- endif -%}

    {{ return(static_analysis_config) }}

{% endmacro %}