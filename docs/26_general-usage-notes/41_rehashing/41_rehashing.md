---
sidebar_position: 41
sidebar_label: Rehashing
title: Rehashing
---

# REHASHING

---

In the context of a Data Vault 2.0 implementation, technical requirements often evolve over time. Maintaining a consistent Raw Data Vault (RDV) is essential when changes occur in the underlying hashing logic. In the `dbt_project.yml`, you can modify different hashing-related global variables such as

- the hashing algorithm (e.g., from MD5 to SHA-256)
- the hash data type
- trimming
- case sensitivity

Changing the hashing function in any way requires either the full refresh of the RDV, if the history is available, or the rehashing of all hash key and hashdiff columns across Hubs, Links, and various Satellite types. The `datavault4dbt` package provides a suite of macros designed to automate this process, ensuring that referential integrity is maintained throughout the transformation.

## CORE CAPABILITIES

The package offers multiple levels of rehashing functionality, ranging from individual entity updates to a full RDV refresh.

### SINGLE ENTITY REHASHING

For targeted updates to specific tables, individual macros can be executed via `dbt run-operation`. This allows for surgical corrections to a single Hub, Link, or Satellite.

```jinja
dbt run-operation rehash_single_hub --args '{
    hub: customer_h, 
    hashkey: HK_CUSTOMER_H,
    business_keys: C_CUSTKEY, 
    overwrite_hash_values: true
}'
```

### BULK REHASHING BY ENTITY TYPE

When processing an entire layer of the RDV, such as all Hubs or all Satellites, the package provides macros that utilize a YAML-based configuration. This approach centralizes the metadata required for the rehashing process.

```jinja
-- models/rehash/rehash_hubs.sql
{{ config(materialized='view') }}
{% set hub_yaml %}
config:
    overwrite_hash_values: true
hubs:
    - name: customer_h
      hashkey: hk_customer_h
      business_keys: [c_custkey]
    - name: order_h
      hashkey: hk_order_h
      business_keys: [order_id]
{% endset %}

{{ datavault4dbt.rehash_hubs(hub_yaml=hub_yaml, drop_old_values=false) }}

SELECT 'success' as status
```

Then, trigger the rehashing by running the model:

```jinja
dbt run -s rehash_hubs
```

With the current setup, the order hub will have the following structure:

| hk_h_order_deprecated                | order_id | ldts       | rsrc   | hk_h_order                         |
|--------------------------------------|----------|------------|--------|------------------------------------|
| 0x5A57D17CD79472395A5731D0E8DB1037   | 2400102  | 2026-02-17 | ORDERS | 0x1F3A375FB2B610DE97F1CA606B4228E0 |
| 0x3E39D22D9FCCC5E133DE1FFDF3E779E0   | 2400163  | 2026-02-19 | ORDERS | 0x65384903A90F4CDBB16C4790B735CBED |
| 0x0D2D4FB1E72DE8A9ACFCF367CC6D0C90   | 2400162  | 2026-02-20 | ORDERS | 0x5F3B349ACB2ED341DC8C84980C6B89CD |

### FULL RDV REHASHING

The `rehash_all_rdv_entities` macro allows for the transformation of multiple entity types within a single execution. It manages the necessary order of operations to maintain consistency across the vault:

1. **Hubs** are processed first to establish the foundation.
2. **Links** are updated next, ensuring they align with the hub hash keys.
3. **Satellites** (Standard, Multi-Active, and Non-Historized) are processed last, recalculating both hash keys and hashdiffs as required.

As with bulk rehashing, this is best implemented within a dedicated model:

```jinja
-- models/rehash/rehash_entire_rdv.sql
{{ config(materialized='view') }}
{% set entity_yaml %}
config:
    overwrite_hash_values: true
hubs:
  - name: customer_h
    hashkey: hk_customer_h
    business_keys: [c_custkey]
links:
  - name: customer_order_l
    link_hashkey: hk_customer_order_l
    hub_config:
        - hub_name: customer_h
          hub_hashkey: hk_customer_h
          business_keys: [c_custkey]
satellites:
  - name: customer_s
    hashkey: hk_customer_h
    hashdiff: hd_customer_s
    parent_entity: customer_h
    payload: [c_name, c_address]
{% endset %}

{{ datavault4dbt.rehash_all_rdv_entities(entity_yaml=entity_yaml, drop_old_values=false) }}

SELECT 'success' as status
```

Execution is performed via:

```jinja
dbt run -s rehash_entire_rdv
```

## TECHNICAL DESIGN AND SAFETY

The rehashing macros include several features to ensure the process is both safe and scalable:

- **Deprecated Column Handling:** By default, the macros do not delete old hash values. Instead, they rename existing columns with a `_deprecated` suffix, allowing for validation of the new hash values before the original data is removed.
- **YAML-Driven Configuration:** Metadata for the rehashing process is defined in YAML, which simplifies maintenance and reduces the need for manual SQL script generation.

## RECOMMENDATIONS FOR OPERATION

To ensure a smooth transition during any rehashing project, the following workflow is recommended:

1. **Start Small:** Perform the initial rehashing on a small subset of your data or a single test entity to verify the configuration and logic.
2. **Use Overwrite, No Drop:** When configuring the macros, set `overwrite_hash_values: true` to replace the primary hash columns but keep `drop_old_values: false`. This ensures that your “old” values are kept as `_deprecated` columns.
3. **Validate Results:** Thoroughly check the results by comparing the new hash values against the `_deprecated` columns.
4. **Cleanup:** Once validation is complete, drop the deprecated columns before the next scheduled run. You can capture the printed dictionary from the dbt log, which lists all columns to be dropped, and create a cleanup model or utilize the `datavault4dbt.custom_alter_relation_add_remove_columns` macro.