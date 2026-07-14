{% docs ref_table %}

## Reference Table

Creates a Reference Table that combines a ref_hub with one or more ref_satellites into a single queryable entity.
The historization mode controls how much history the table retains: 'latest' keeps only the current record,
'full' keeps all historical records, and 'snapshot' aligns with a snapshot table.

### Usage (latest — current values only)

```jinja
{{ datavault4dbt.ref_table(
    ref_hub='country_rh',
    ref_satellites=['country_1_rs'],
    historized='latest'
) }}
```

### Usage (snapshot-based)

```jinja
{{ datavault4dbt.ref_table(
    ref_hub='country_rh',
    ref_satellites=['country_1_rs'],
    historized='snapshot',
    snapshot_relation='control_snap_v1'
) }}
```

### Metadata block usage

`meta` is a Jinja string variable containing all macro parameters as YAML. Define it using a
`set` block in the model SQL file — the macro parses the YAML string at runtime:

```jinja
{%- set meta -%}
ref_hub: 'country_rh'
ref_satellites:
  - country_1_rs
historized: 'latest'
{%- endset -%}

{{ datavault4dbt.ref_table(yaml_metadata=meta) }}
```

{% enddocs %}
