{% docs eff_sat_v0 %}

## Effectivity Satellite v0

Creates an Effectivity Satellite that tracks the active/inactive status of a link relationship over time.
An effectivity satellite contains a boolean flag ('is_active') that indicates whether a given relationship
was active at each load timestamp.

Note: `source_is_single_batch` is a required parameter — it must be explicitly set to `true` or `false`.

### Usage

```jinja
{% raw %}
{{ datavault4dbt.eff_sat_v0(
    source_model='stg_account_contact',
    tracked_hashkey='hk_account_contact_l',
    source_is_single_batch=false
) }}
{% endraw %}
```

### With custom alias for the active flag

```jinja
{% raw %}
{{ datavault4dbt.eff_sat_v0(
    source_model='stg_account_contact',
    tracked_hashkey='hk_account_contact_l',
    is_active_alias='is_active',
    source_is_single_batch=false
) }}
{% endraw %}
```

### Metadata block usage

`meta` is a Jinja string variable containing all macro parameters as YAML. Define it using a
`set` block in the model SQL file — the macro parses the YAML string at runtime:

```jinja
{% raw %}
{%- set meta -%}
source_model: 'stg_account_contact'
tracked_hashkey: 'hk_account_contact_l'
source_is_single_batch: false
{%- endset -%}

{{ datavault4dbt.eff_sat_v0(yaml_metadata=meta) }}
{% endraw %}
```

{% enddocs %}
