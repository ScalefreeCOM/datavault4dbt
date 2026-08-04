{% docs sat_v1 %}

## Satellite v1

Calculates a virtualized load end date on top of a version 0 satellite. This column is generated for usage in the
PIT tables, and only virtualized to follow the insert-only approach. A version 1 satellite should be materialized
as a view by default. Usually one version 1 sat would be created for each version 0 sat.

### Usage

```jinja
{% raw %}
{{ datavault4dbt.sat_v1(
    sat_v0='account_data_sfdc_0_s',
    hashkey='hk_account_h',
    hashdiff='hd_account_data_sfdc_s'
) }}
{% endraw %}
```

### With is_current flag

```jinja
{% raw %}
{{ datavault4dbt.sat_v1(
    sat_v0='account_data_sfdc_0_s',
    hashkey='hk_account_h',
    hashdiff='hd_account_data_sfdc_s',
    add_is_current_flag=true
) }}
{% endraw %}
```

### Metadata block usage

`meta` is a Jinja string variable containing all macro parameters as YAML. Define it using a
`set` block in the model SQL file — the macro parses the YAML string at runtime:

```jinja
{% raw %}
{%- set meta -%}
sat_v0: 'account_data_sfdc_0_s'
hashkey: 'hk_account_h'
hashdiff: 'hd_account_data_sfdc_s'
{%- endset -%}

{{ datavault4dbt.sat_v1(yaml_metadata=meta) }}
{% endraw %}
```

{% enddocs %}
