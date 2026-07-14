{% docs ma_sat_v0 %}

## Multi-Active Satellite v0

Creates a multi-active satellite version 0, materialized as an incremental table. Applied on top of the staging layer,
connected to either a Hub or a Link. A multi-active satellite stores multiple concurrently valid records per parent
hashkey, distinguished by a multi-active key (e.g. multiple phone numbers per contact).

On top of each version 0 multi-active satellite, a version 1 should be created using the ma_sat_v1 macro.
If a stage model is defined as multi-active, all satellites out of that stage model must be implemented as
multi-active satellites.

Features:
- Can handle multiple updates per batch, without losing intermediate changes — initial loading is supported.
- Uses a dynamic high-water-mark to optimize loading performance of multiple loads.

### Usage

```jinja
{{ datavault4dbt.ma_sat_v0(
    parent_hashkey='hk_contact_h',
    src_hashdiff='hd_contact_phonenumber_s',
    src_ma_key='phonetype',
    src_payload=['phone_number', 'is_primary'],
    source_model='stage_contact'
) }}
```

### With composite multi-active key

```jinja
{{ datavault4dbt.ma_sat_v0(
    parent_hashkey='hk_contact_h',
    src_hashdiff='hd_contact_phonenumber_s',
    src_ma_key=['phonetype', 'iid'],
    src_payload=['phone_number'],
    source_model='stage_contact'
) }}
```

### Metadata block usage

`meta` is a Jinja string variable containing all macro parameters as YAML. Define it using a
`set` block in the model SQL file — the macro parses the YAML string at runtime:

```jinja
{%- set meta -%}
parent_hashkey: 'hk_contact_h'
src_hashdiff: 'hd_contact_phonenumber_s'
src_ma_key: 'phonetype'
src_payload:
  - phone_number
  - is_primary
source_model: 'stage_contact'
{%- endset -%}

{{ datavault4dbt.ma_sat_v0(yaml_metadata=meta) }}
```

{% enddocs %}
