{% docs hub %}

## Hub

Creates a Hub entity in the Raw Data Vault. A Hub captures and stores the unique business keys for a business concept,
along with the first load date and record source. Each business key combination is stored only once.

Features:
- Loadable by multiple sources
- Supports multiple updates per batch and therefore initial loading
- Can use a dynamic high-water-mark to optimize loading performance of multiple loads
- Allows source mappings for deviations between source column names and hub column names

### Usage

```jinja
{% raw %}
{{ datavault4dbt.hub(
    hashkey='hk_account_h',
    business_keys='account_id',
    source_models={
        'stg_account_crm': {
            'bk_columns': 'account_id',
            'rsrc_static': '*/CRM/Accounts/*'
        },
        'stg_account_erp': {
            'bk_columns': 'account_id',
            'rsrc_static': '*/ERP/Accounts/*'
        }
    }
) }}
{% endraw %}
```

### Metadata block usage

`meta` is a Jinja string variable containing all macro parameters as YAML. Define it using a
`set` block in the model SQL file — the macro parses the YAML string at runtime:

```jinja
{% raw %}
{%- set meta -%}
hashkey: 'hk_account_h'
business_keys: account_id
source_models:
  stg_account_crm:
    bk_columns: account_id
    rsrc_static: '*/CRM/Accounts/*'
  stg_account_erp:
    bk_columns: account_id
    rsrc_static: '*/ERP/Accounts/*'
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=meta) }}
{% endraw %}
```

{% enddocs %}
