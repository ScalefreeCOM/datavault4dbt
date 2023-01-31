{%- macro source_model_processing(source_models, parameters, set_rsrc_static=true) -%}

    {%- set ns = namespace(source_model_list = [], source_model_dict = {}, source_model_input = [], has_rsrc_static_defined=true, source_models_rsrc_dict = {}) -%}

    {%- set dict_result = {} -%}

    {%- if source_models is mapping -%}

        {%- for source_model in source_models.keys() - %}

            {%- set source_model_dict = source_models[source_model] -%}
            {%- do source_model_dict.update({'name': source_model}) -%}

            {%- do ns.source_model_list.append(source_model_dict) -%}

        {%- endfor -%}

    {%- elif not datavault4dbt.is_list(source_models) -%}

        {%- for parameter, input in parameters.items() -%}
            {% do ns.source_model_dict.update({parameter: input}) %}
        {%- endfor -%}

        {%- do ns.source_model_list.append(ns.source_model_dict) -%}

    {%- elif datavault4dbt.is_list(source_models) -%}

        {%- set ns.source_model_list = source_models -%}

    {%- endif -%}

    {%- set id = 1 -%}

    {%- for source_model in ns.source_model_list -%}

        {%- do source_model.update({'id': id}) -%}

        {%- for parameter, input in parameters -%}

            {%- if parameter not in source_model.keys() -%}
                {%- do source_model.update({parameter: input}) -%}
            {%- endif -%}

        {%- endif -%}

        {%- if set_rsrc_static -%}

            {%- if 'rsrc_static' not in source_model.keys() -%}
                {%- set ns.has_rsrc_static_defined = false -%}
            {%- else -%}

                {%- if not (source_model['rsrc_static'] is iterable and source_model['rsrc_static'] is not string) -%}

                    {%- if source_model['rsrc_static']  == '' or source_model['rsrc_static'] is none -%}
                        {%- if execute -%}
                            {{ exceptions.raise_compiler_error("If rsrc_static is defined -> it must not be an empty string ") }}
                        {%- endif %}
                    {%- else -%}
                        {%- do ns.source_models_rsrc_dict.update({id : [source_model['rsrc_static']] } ) -%}
                    {%- endif -%}

                {%- elif source_model['rsrc_static']  is iterable -%}
                    {%- do ns.source_models_rsrc_dict.update({id : source_model['rsrc_static']  } ) -%}
                {%- endif -%}

            {%- endif -%}

        {%- endif -%}

        {%- if 'hk_column' not in source_model.keys() -%}
            {%- do source_model.update({'hk_column': hashkey}) -%}
        {%- endif -%}

        {%- if 'bk_columns' in source_models[source_model].keys() -%}
            {%- set bk_column_input = source_models[source_model]['bk_columns'] -%}

            {%- if not (bk_column_input is iterable and bk_column_input is not string) -%}
                {%- set bk_column_input = [bk_column_input] -%}
            {%- endif -%}

            {%- do source_models[source_model].update({'bk_columns': bk_column_input}) -%}
        {%- elif not datavault4dbt.is_list(bk_column_input) -%}
            {%- set bk_list = datavault4dbt.expand_column_list(columns=[bk_column_input]) -%}
            {%- do source_models[source_model].update({'bk_columns': bk_list}) -%}
        {%- else -%}{%- do source_models[source_model].update({'bk_columns': business_keys}) -%}
        {%- endif -%}

    {%- set id = id + 1 -%}

    {%- endfor -%}

    {%- do dict_result.update({"source_model_list": ns.source_model_list, "has_rsrc_static_defined": ns.has_rsrc_static_defined, "source_models_rsrc_dict": ns.source_models_rsrc_dict }) -%}

    {{ return(dict_result | tojson) }}

    {%- endmacro -%}
