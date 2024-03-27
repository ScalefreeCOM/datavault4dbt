{%- macro source_model_processing(source_models, set_rsrc_static=true, parameters={}, business_keys=none, reference_keys=none, foreign_hashkeys=none, payload=none) -%}

    {%- set ns_source_models = namespace(source_model_list = [], source_model_list_tmp=[], source_model_dict = {}, source_model_input = [], has_rsrc_static_defined=true, source_models_rsrc_dict = {}) -%}

    {%- set dict_result = {} -%}

    {{ log('ref_keys: '~reference_keys, false)}}

    {%- if source_models is mapping -%}

        {%- for source_model in source_models.keys() -%}

            {%- set source_model_dict = source_models[source_model] -%}
            {%- do source_model_dict.update({'name': source_model}) -%}

            {%- do ns_source_models.source_model_list.append(source_model_dict) -%}

        {%- endfor -%}

    {%- elif not datavault4dbt.is_list(source_models) -%}

        {%- if parameters.keys()|length > 0 -%}

            {%- for parameter, input in parameters.items() -%}
                {% do ns_source_models.source_model_dict.update({parameter: input}) %}
            {%- endfor -%}

        {%- endif -%}

        {%- do ns_source_models.source_model_list.append(ns_source_models.source_model_dict) -%}

    {%- elif datavault4dbt.is_list(source_models) -%}

        {%- set ns_source_models.source_model_list = source_models -%}

    {%- endif -%}

    {%- for source_model in ns_source_models.source_model_list -%}

        {%- do source_model.update({'id': loop.index}) -%}

        {%- if parameters.keys()|length > 0 -%}
            
            {%- for parameter, input in parameters.items() -%}

                {%- if parameter not in source_model.keys() -%}
                    {%- do source_model.update({parameter: input}) -%}
                {%- endif -%}

            {%- endfor -%}

        {%- endif -%}

        {%- if set_rsrc_static -%}

            {%- if 'rsrc_static' not in source_model.keys() -%}
                {%- set ns_source_models.has_rsrc_static_defined = false -%}
            {%- else -%}

                {%- if not (source_model['rsrc_static'] is iterable and source_model['rsrc_static'] is not string) -%}

                    {%- if source_model['rsrc_static']  == '' or source_model['rsrc_static'] is none -%}
                        {%- if execute -%}
                            {{ exceptions_source_models.raise_compiler_error("If rsrc_static is defined -> it must not be an empty string ") }}
                        {%- endif %}
                    {%- else -%}
                        {%- do ns_source_models.source_models_rsrc_dict.update({loop.index : [source_model['rsrc_static']] } ) -%}
                    {%- endif -%}

                {%- elif source_model['rsrc_static']  is iterable -%}
                    {%- do ns_source_models.source_models_rsrc_dict.update({loop.index : source_model['rsrc_static']  } ) -%}
                {%- endif -%}

            {%- endif -%}

        {%- endif -%}

        {%- if business_keys is not none -%}
            {%- set bk_column_input = business_keys -%}

            {%- if 'bk_columns' in source_model.keys() -%}
                {%- set bk_column_input = source_model['bk_columns'] -%}

                {%- if not (bk_column_input is iterable and bk_column_input is not string) -%}
                    {%- set bk_column_input = [bk_column_input] -%}
                {%- endif -%}

                {%- do source_model.update({'bk_columns': bk_column_input}) -%}
            {%- elif not datavault4dbt.is_list(bk_column_input) -%}
                {%- set bk_list = datavault4dbt.expand_column_list(columns=[bk_column_input]) -%}
                {%- do source_model.update({'bk_columns': bk_list}) -%}
            {%- else -%}{%- do source_model.update({'bk_columns': business_keys}) -%}
            {%- endif -%}

        {%- endif -%}

        {%- if reference_keys is not none -%}
            {%- set ref_column_input = reference_keys -%}

            {%- if 'ref_keys' in source_model.keys() -%}
                {%- set ref_column_input = source_model['ref_keys'] -%}

                {%- if not (ref_column_input is iterable and ref_column_input is not string) -%}
                    {%- set ref_column_input = [ref_column_input] -%}
                {%- endif -%}

                {%- do source_model.update({'ref_keys': ref_column_input}) -%}
            {%- elif not datavault4dbt.is_list(ref_column_input) -%}
                {%- set rk_list = datavault4dbt.expand_column_list(columns=[ref_column_input]) -%}
                {%- do source_model.update({'ref_keys': rk_list}) -%}
            {%- else -%}{%- do source_model.update({'ref_keys': ref_column_input}) -%}
            {%- endif -%}

        {%- endif -%}       

        {%- if foreign_hashkeys is not none -%}
            {%- set foreign_hashkeys_input = foreign_hashkeys -%}

            {%- if 'fk_columns' in source_model.keys() -%}
                {%- set foreign_hashkeys_input = source_model['fk_columns'] -%}

                {%- if not (foreign_hashkeys_input is iterable and foreign_hashkeys_input is not string) -%}
                    {%- set foreign_hashkeys_input = [foreign_hashkeys_input] -%}
                {%- endif -%}

                {%- do source_model.update({'fk_columns': foreign_hashkeys_input}) -%}
            {%- elif not datavault4dbt.is_list(foreign_hashkeys_input) -%}
                {%- set fk_list = datavault4dbt.expand_column_list(columns=[foreign_hashkeys_input]) -%}
                {%- do source_model.update({'fk_columns': fk_list}) -%}
            {%- else -%}{%- do source_model.update({'fk_columns': foreign_hashkeys_input}) -%}
            {%- endif -%}

        {%- endif -%}     

        {%- if payload is not none -%}
            {%- set payload_input = payload -%}

            {%- if 'payload' in source_model.keys() -%}
                {%- set payload_input = source_model['payload'] -%}

                {%- if not (payload_input is iterable and payload_input is not string) -%}
                    {%- set payload_input = [payload_input] -%}
                {%- endif -%}

                {%- do source_model.update({'payload': payload_input}) -%}
            {%- elif not datavault4dbt.is_list(payload_input) -%}
                {%- set payload_list = datavault4dbt.expand_column_list(columns=[payload_input]) -%}
                {%- do source_model.update({'payload': payload_list}) -%}
            {%- else -%}{%- do source_model.update({'payload': payload_input}) -%}
            {%- endif -%}

        {%- endif -%}                  

        {%- do ns_source_models.source_model_list_tmp.append(source_model) -%}

    {%- endfor -%}

    {%- do dict_result.update({"source_model_list": ns_source_models.source_model_list_tmp ,"has_rsrc_static_defined": ns_source_models.has_rsrc_static_defined, "source_models_rsrc_dict": ns_source_models.source_models_rsrc_dict}) -%}
     
     {{log('dict_result: '~ dict_result, false)}}

    {{ return(dict_result | tojson) }}

{%- endmacro -%}
