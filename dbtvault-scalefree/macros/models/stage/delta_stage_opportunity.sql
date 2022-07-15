{{ config(schema='public_release_test', materialized='table')}}

{%- set all_columns = adapter.get_columns_in_relation(ref('stage_opportunity')) -%}
{%- set exclude_columns = ['ldts', 'hk_opportunity_h', 'opportunity_key__c'] -%}

{%- set new_rows = [['2022-05-21T00-00-01', '6daf33b83a9469f236fb5e057a8238ce', 'O-1234'],
                    ['2022-05-21T00-00-01', '6daf33b83a9469f236fb5e057a8238cf', 'O-2234'],
                    ['2022-05-21T00-00-01', '6daf33b83a9469f236fb5e057a8238cg', 'O-3234'],
                    ['2022-05-21T00-00-01', '6daf33b83a9469f236fb5e057a8238ch', 'O-4234'],
                    ['2022-06-21T00-00-01', '6daf33b83a9469f236fb5e057a8238ci', 'O-5234'],
                    ['2022-06-21T00-00-01', '6daf33b83a9469f236fb5e057a8238cj', 'O-6234'],
                    ['2022-06-21T00-00-01', '6daf33b83a9469f236fb5e057a8238ck', 'O-7234'],
                    ['2022-07-21T00-00-01', '6daf33b83a9469f236fb5e057a8238cl', 'O-8234'],
                    ['2022-07-21T00-00-01', '6daf33b83a9469f236fb5e057a8238cm', 'O-9234'],
                    ['2022-07-21T00-00-01', '6daf33b83a9469f236fb5e057a8238cn', 'O-1235']] -%}

WITH original_data AS (
    SELECT * FROM {{ ref('stage_opportunity') }}
),

{% for row in new_rows %}
    row_{{ loop.index }} AS (SELECT
    {% set ldts = row[0] -%}
    {%- set hk = row[1] -%}
    {%- set bk = row[2] -%}

        {% for column in all_columns -%}
        {% if column.name == 'ldts' -%}
            PARSE_TIMESTAMP('%Y-%m-%dT%H-%M-%S', '{{ ldts }}'),
        {% elif column.name == 'hk_opportunity_h' -%}
            '{{ hk }}',
        {% elif column.name|lower() == 'opportunity_key__c' -%}
            '{{ bk }}',
        {% else -%}
            {{ column.name }}{{',' if not loop.last }}
        {% endif %}
        {%- endfor -%}
    FROM {{ ref('stage_opportunity') }} LIMIT 1){{',' if not loop.last }}
    {%- endfor -%}

SELECT * FROM original_data
{% for row in new_rows %}
UNION ALL 
SELECT * FROM row_{{ loop.index }}
{% endfor %}

