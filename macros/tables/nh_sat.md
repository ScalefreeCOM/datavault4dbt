{% docs nh_sat %}

## Non-Historized Satellite

Creates a non-historized satellite, materialized as an incremental table. Applied on top of the staging layer,
connected to either a Hub or a Link. Besides the missing hashdiff, a non-historized satellite applies the same
loading logic as a regular version 0 satellite. Each satellite can only be loaded by one source model.

Features:
- High-performance loading of non-historized satellite data.

### Usage

```jinja
{{ datavault4dbt.nh_sat(
    parent_hashkey='hk_account_h',
    src_payload=['name', 'address', 'country', 'phone', 'email'],
    source_model='stage_account'
) }}
```

### Metadata block usage

`meta` is a Jinja string variable containing all macro parameters as YAML. Define it using a
`set` block in the model SQL file — the macro parses the YAML string at runtime:

```jinja
{%- set meta -%}
parent_hashkey: 'hk_account_h'
src_payload:
  - name
  - address
  - country
  - phone
  - email
source_model: 'stage_account'
{%- endset -%}

{{ datavault4dbt.nh_sat(yaml_metadata=meta) }}
```

{% enddocs %}
