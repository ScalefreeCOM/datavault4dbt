{% docs link %}

## Link

Creates a Link entity in the Raw Data Vault. A Link connects two or more entities, or an entity with itself.
It can be loaded by one or more source staging tables if multiple sources share the same business definitions.
If multiple sources are used, they must all have the same number of foreign keys.

### Usage

```jinja
{{ datavault4dbt.link(
    link_hashkey='hk_account_contact_l',
    foreign_hashkeys=['hk_account_h', 'hk_contact_h'],
    source_models={
        'stg_account_contact': {
            'rsrc_static': '*/CRM/AccountContact/*'
        }
    }
) }}
```

### Metadata block usage

```jinja
{{ datavault4dbt.link(yaml_metadata=meta) }}
```

{% enddocs %}
