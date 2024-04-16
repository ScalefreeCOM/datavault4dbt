{% macro string_to_timestamp(format, timestamp) %}
{{return(adapter.dispatch('string_to_timestamp', 'datavault4dbt')(format=format,
                                                                        timestamp= timestamp)) }}
{%- endmacro -%}

{%- macro default__string_to_timestamp(format, timestamp) -%}
    PARSE_TIMESTAMP('{{ format }}', '{{ timestamp }}')
{%- endmacro -%}

{%- macro exasol__string_to_timestamp(format, timestamp) -%}
    TO_TIMESTAMP('{{ timestamp }}', '{{ format }}')
{%- endmacro -%}

{%- macro snowflake__string_to_timestamp(format, timestamp) -%}
    TO_TIMESTAMP('{{ timestamp }}', '{{ format }}')
{%- endmacro -%}

{%- macro synapse__string_to_timestamp(format, timestamp) -%}
    CONVERT(datetime2, '{{ timestamp }}', {{ format }})
{%- endmacro -%}

{%- macro postgres__string_to_timestamp(format, timestamp) -%}
    TO_TIMESTAMP('{{ timestamp }}', '{{ format }}')
{%- endmacro -%}

{%- macro redshift__string_to_timestamp(format, timestamp) -%}
    TO_TIMESTAMP('{{ timestamp }}', '{{ format }}')
{%- endmacro -%}

