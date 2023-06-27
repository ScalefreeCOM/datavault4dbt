{%- macro multikey(columns, prefix=none, condition=none, operator='AND', right_columns=none) -%}

    {{- adapter.dispatch('multikey', 'datavault4dbt')(columns=columns, prefix=prefix, condition=condition, operator=operator, right_columns=right_columns) -}}

{%- endmacro %}

{%- macro default__multikey(columns, prefix=none, condition=none, operator='AND', right_columns=none) -%}

    {%- if prefix is string -%}
        {%- set prefix = [prefix] -%}
    {%- endif -%}

    {%- if columns is string -%}
        {%- set columns = [columns] -%}
    {%- endif -%}

    {%- if right_columns is none -%}
        {%- set right_columns = columns -%}
    {%- elif right_columns is string -%}
        {%- set right_columns = [right_columns] -%}
    {%- elif right_columns|length != columns|length -%}
        {%- set error_message -%}
      Multikey Error: If right_columns are defined, it must be the same length as columns. 
      Got: 
        Columns: {{ columns }} with length {{ columns|length }}
        right_columns: {{ right_columns }} with length {{ right_columns|length }}
        {%- endset -%}

        {{- exceptions.raise_compiler_error(error_message) -}}
    {%- endif -%}

    {%- if condition in ['<>', '!=', '='] -%}
        {%- for col in columns -%}
            {%- if prefix -%}
                {{- datavault4dbt.prefix([col], prefix[0], alias_target='target') }} {{ condition }} {{ datavault4dbt.prefix([right_columns[loop.index0]], prefix[1]) -}}
            {%- endif %}
            {%- if not loop.last %} {{ operator }} {% endif -%}
        {% endfor -%}
    {%- else -%}
        {%- if datavault4dbt.is_list(columns) -%}
            {%- for col in columns -%}
                {{ (prefix[0] ~ '.') if prefix }}{{ col }} {{ condition if condition else '' }}
                {%- if not loop.last -%} {{ "\n    " ~ operator }} {% endif -%}
            {%- endfor -%}
        {%- else -%}
            {{ prefix[0] ~ '.' if prefix }}{{ columns }} {{ condition if condition else '' }}
        {%- endif -%}
    {%- endif -%}

{%- endmacro -%}