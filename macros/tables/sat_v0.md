{% docs sat_v0 %}

## Satellite v0

Creates a standard satellite version 0, materialized as an incremental table. Applied on top of the staging layer,
connected to either a Hub or a Link. Each satellite can only be loaded by one source model, since we typically
recommend a satellite split by source system.

On top of each version 0 satellite, a version 1 satellite should be created using the sat_v1 macro, which extends
the v0 satellite by a virtually calculated load end date.

Features:
- Can handle multiple updates per batch, without losing intermediate changes — initial loading is supported.
- Uses a dynamic high-water-mark to optimize loading performance of multiple loads.

### Usage

```jinja
{{ datavault4dbt.sat_v0(
    parent_hashkey='hk_account_h',
    src_hashdiff='hd_account_data_sfdc_s',
    src_payload=['name', 'address', 'country', 'phone', 'email'],
    source_model='stage_account'
) }}
```

### Metadata block usage

```jinja
{{ datavault4dbt.sat_v0(yaml_metadata=meta) }}
```

{% enddocs %}
