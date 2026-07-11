{% docs rec_track_sat %}

## Record Tracking Satellite

Creates a Record Tracking Satellite, most commonly used to track the appearances of hashkeys (calculated out of
business keys) inside one or multiple source systems. This can either be the hashkey of a hub, or the hashkey
of a link. Typically if a hub is loaded from three sources, the corresponding Record Tracking Satellite would
track the same three sources.

Features:
- Tracks the appearance of a specific hashkey in one or more staging areas.
- Allows source mappings for deviations between the hashkey name inside the stages and the target.
- Supports multiple updates per batch and therefore initial loading.
- Uses a dynamic high-water-mark to optimize loading performance of multiple loads.
- Can track either link or hub hashkeys.

### Usage

```jinja
{{ datavault4dbt.rec_track_sat(
    tracked_hashkey='hk_contact_h',
    source_models={
        'stg_contact_crm': {
            'rsrc_static': '*/CRM/Contact/*'
        },
        'stg_contact_erp': {
            'hk_column': 'hk_contact_erp_h',
            'rsrc_static': '*/ERP/Contact/*'
        }
    }
) }}
```

### Metadata block usage

```jinja
{{ datavault4dbt.rec_track_sat(yaml_metadata=meta) }}
```

{% enddocs %}
