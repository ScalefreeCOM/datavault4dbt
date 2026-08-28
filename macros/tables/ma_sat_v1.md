{% docs ma_sat_v1 %}

## Multi-Active Satellite v1

Calculates the load end dates for multi-active data, based on a multi-active attribute. Must be based on a version 0
multi-active satellite, that would then hold multiple records per hashkey+ldts combination.

A version 1 multi-active satellite should be materialized as a view by default.

Features:
- Calculates virtualized load-end-dates to correctly identify multiple active records per batch.
- Enforces insert-only approach by view materialization.
- Allows multiple attributes to be used as the multi-active-attribute.

### Usage

```jinja
{% raw %}
{{ datavault4dbt.ma_sat_v1(
    sat_v0='contact_phonenumber_0_s',
    hashkey='hk_contact_h',
    hashdiff='hd_contact_phonenumber_s',
    ma_attribute='phone_type'
) }}
{% endraw %}
```

### With composite multi-active attribute

```jinja
{% raw %}
{{ datavault4dbt.ma_sat_v1(
    sat_v0='contact_phonenumber_0_s',
    hashkey='hk_contact_h',
    hashdiff='hd_contact_phonenumber_s',
    ma_attribute=['phone_type', 'iid']
) }}
{% endraw %}
```

### Metadata block usage

`meta` is a Jinja string variable containing all macro parameters as YAML. Define it using a
`set` block in the model SQL file — the macro parses the YAML string at runtime:

```jinja
{% raw %}
{%- set meta -%}
sat_v0: 'contact_phonenumber_0_s'
hashkey: 'hk_contact_h'
hashdiff: 'hd_contact_phonenumber_s'
ma_attribute: 'phone_type'
{%- endset -%}

{{ datavault4dbt.ma_sat_v1(yaml_metadata=meta) }}
{% endraw %}
```

{% enddocs %}
