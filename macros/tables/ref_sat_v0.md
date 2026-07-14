{% docs ref_sat_v0 %}

## Reference Satellite v0

Creates a version 0 Reference Satellite in the Raw Data Vault. A Reference Satellite stores the descriptive
attributes (payload) of a reference entity over time, tracking every change. It is linked to its parent
Reference Hub via the reference key.

### Usage

```jinja
{{ datavault4dbt.ref_sat_v0(
    parent_ref_keys='country_code',
    src_hashdiff='hd_country_rs',
    src_payload=['country_name', 'continent', 'region'],
    source_model='stg_country'
) }}
```

### Metadata block usage

`meta` is a Jinja string variable containing all macro parameters as YAML. Define it using a
`set` block in the model SQL file — the macro parses the YAML string at runtime:

```jinja
{%- set meta -%}
parent_ref_keys: 'country_code'
src_hashdiff: 'hd_country_rs'
src_payload:
  - country_name
  - continent
  - region
source_model: 'stg_country'
{%- endset -%}

{{ datavault4dbt.ref_sat_v0(yaml_metadata=meta) }}
```

{% enddocs %}
