{% docs nh_link %}

## Non-Historized Link

Creates a non-historized (formerly transactional) link entity, connecting two or more entities, or a transactional
fact of one entity. It can be loaded by one or more source staging tables if multiple sources share the same
business definitions.

In the background a non-historized link uses exactly the same loading logic as a regular link, but adds the
descriptive attributes as additional payload.

### Usage

```jinja
{{ datavault4dbt.nh_link(
    link_hashkey='hk_transaction_account_nl',
    foreign_hashkeys=['hk_transaction_h', 'hk_account_h'],
    payload=['currency_isocode', 'amount', 'purpose', 'transaction_date'],
    source_models={
        'stg_transaction': {
            'rsrc_static': '*/ERP/Transactions/*'
        }
    }
) }}
```

### Metadata block usage

```jinja
{{ datavault4dbt.nh_link(yaml_metadata=meta) }}
```

{% enddocs %}
