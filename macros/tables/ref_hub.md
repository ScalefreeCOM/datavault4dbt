{% docs ref_hub %}

## Reference Hub

Creates a Reference Hub entity in the Raw Data Vault. A Reference Hub stores the unique reference keys for
reference data (e.g. country codes, product categories) that does not originate from a business entity
but is used to classify and enrich other entities.

### Usage

```jinja
{{ datavault4dbt.ref_hub(
    ref_keys='country_code',
    source_models={
        'stg_country': {
            'ref_keys': 'country_code',
            'rsrc_static': '*/REF/Country/*'
        }
    }
) }}
```

### Metadata block usage

`meta` is a Jinja string variable containing all macro parameters as YAML. Define it using a
`set` block in the model SQL file — the macro parses the YAML string at runtime:

```jinja
{%- set meta -%}
ref_keys: 'country_code'
source_models:
  stg_country:
    ref_keys: country_code
    rsrc_static: '*/REF/Country/*'
{%- endset -%}

{{ datavault4dbt.ref_hub(yaml_metadata=meta) }}
```

{% enddocs %}
