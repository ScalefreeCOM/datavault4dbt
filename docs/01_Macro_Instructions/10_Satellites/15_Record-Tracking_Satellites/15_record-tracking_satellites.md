---
sidebar_position: 15
sidebar_label: Record-Tracking Satellite
title: Record-Tracking Satellite
---

# RECORD-TRACKING SATELLITE

---

This macro creates a Record Tracking Satellite and is most commonly used to track the appearances of hashkeys (calculated out of business keys) inside one or multiple source systems. This can either be the hashkey of a hub, or the hashkey of a link. Therefore Record Tracking Satellites can be build both for Hubs and Links. Typically if a hub is loaded from three sources, the corresponding Record Tracking Satellite would track the same three sources, since they apparently share the same business definition. If the record tracking satellite is loaded by multiple sources, for each source a rsrc_static must be defined, and optionally the name of the hashkey column inside that source, if it deviates between sources.

Features:

- Tracks the appearance of a specific hashkey in one or more staging areas
- Allows source mappings for deviations between the hashkey name inside the stages and the target
- Supports multiple updates per batch and therefore initial loading
- Using a dynamic high-water-mark to optimize loading performance of multiple loads
- Can either track link- or hub-hashkeys

### REQUIRED PARAMETERS

| Parameters      | Data Type                                   | Required  | Default Value | Explanation |
|-----------------|---------------------------------------------|-----------|---------------|-------------|
| tracked_hashkey | string                                      | mandatory | –             | The name of the hashkey column you want to track. Needs to be available in the underlying staging layer. If you want to track multiple hashkeys out of one stage, you need to create one record tracking satellite for each hashkey. |
| source_models   | string \| list of dictionaries \| dictionary | mandatory | –             | For a single source entity, a string with the name of the source staging model is required. For multi source entities, please see: Multi-Source-Models. The inner dictionaries need to have `name` as a key, and optionally the keys `rsrc_static` and `hk_column`. |

### OPTIONAL PARAMETERS

| Parameters   | Data Type | Required | Default Value              | Explanation |
|--------------|-----------|----------|----------------------------|-------------|
| disable_hwm  | boolean   | optional | False                      | Whether the automatic application of a High-Water Mark (HWM) should be disabled or not. By default this is set to false, so the HWM is enabled. |
| src_ldts     | string    | optional | datavault4dbt.ldts_alias   | Name of the ldts column inside the source models. Needs to use the same column name as defined as alias inside the staging model. |
| src_rsrc     | string    | optional | datavault4dbt.rsrc_alias   | Name of the rsrc column inside the source models. Is optional, will use the global variable `datavault4dbt.rsrc_alias`. Needs to use the same column name as defined as alias inside the staging model. |
| src_stg      | string    | optional | datavault4dbt.stg_alias    | Name of the column containing information about the source stage model. |

## EXAMPLE 1

```jinja
{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
tracked_hashkey: hk_customer_h
source_models: stg_customers
{%- endset -%}    

{{ datavault4dbt.rec_track_sat(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

With this example, a record tracking satellite for the hashkey hk_customer_h inside the stage stg_customers is created.

- **tracked_hashkey**:
  - __hk_customer_h__: This hashkey column belongs to the customer hub and is available inside the defined staging model.
- **source_models**:
  - __stg_customers__: Since the customer_h is only loaded from one source, the corresponding record tracking satellite is also only loaded from one source. rsrc_static is not defined, because it is not required for single source entities.

## EXAMPLE 2

```jinja
{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
tracked_hashkey: hk_customer_h
source_models:
    - name: stg_customers
      rsrc_static: 'TPC_H_SF1.Customer'
    - name: stg_suppliers
      hk_column: hk_supplier_h
      rsrc_static: 'TPC_H_SF1.Supplier'
{%- endset -%}    

{{ datavault4dbt.rec_track_sat(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

With this example, a record tracking satellite for the customer hub, which is loaded by two stages. The customer hashkey inside the hub is apparently named differently across both source stage models.

- **tracked_hashkey**:
  - __hk_customer_h__: This hashkey column belongs to the customer hub and is used as a column name inside the record tracking satellite.
- **source_models**:
  - __stg_customers__: Since the customer_h is loaded from two source models, the record tracking satellite should track exactly the same sources. Inside __stg_supplier__ the column used for the customer hashkey is called unlike inside __stg_customer__. Therefore the actual name inside the stage is defined as __hk_column__ for __stg_suppliers__.

### DISABLING HIGH-WATER MARK

The High-Water Mark can be disabled safely, but typically would decrease the performance again.

We recommend to **try a bit what works best in your environment**. You basically have three options:

- **Keep HWM activated**, for multi-source Record-Tracking-Satellites this would require the rsrc_static to be defined for each source. For single source Record-Tracking-Satellites, nothing needs to be done, the HWM is activated automatically.
- **Disable the HWM entirely.** For multi-source Record-Tracking-Satellites you just need to not specify the rsrc_static attribute. For single-source Record-Tracking-Satellites you need to add the parameter disable_hwm=true to your Record-Tracking-Satellite macro call.
- **Move the HWM to a previous layer.** First, you apply the previous step to disable the HWM in the Record-Tracking-Satellites. Then you implement some kind of mechanism in previous dbt layers to ensure that only records newer than what you already processed are available there. This could be especially effective when combining with different materializations of these previous layers.