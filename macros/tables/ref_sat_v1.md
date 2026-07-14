{% docs ref_sat_v1 %}

## Reference Satellite v1

Creates a version 1 Reference Satellite view derived from an existing ref_sat_v0. Adds a load end date timestamp
(ledts) column, enabling point-in-time queries without window functions. A version 1 ref satellite should be
materialized as a view.

### Usage

```jinja
{{ datavault4dbt.ref_sat_v1(
    ref_sat_v0='country_0_rs',
    ref_keys='country_code',
    hashdiff='hd_country_rs'
) }}
```

### Metadata block usage

`meta` is a Jinja string variable containing all macro parameters as YAML. Define it using a
`set` block in the model SQL file — the macro parses the YAML string at runtime:

```jinja
{%- set meta -%}
ref_sat_v0: 'country_0_rs'
ref_keys: 'country_code'
hashdiff: 'hd_country_rs'
{%- endset -%}

{{ datavault4dbt.ref_sat_v1(yaml_metadata=meta) }}
```

{% enddocs %}
