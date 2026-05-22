---
sidebar_position: 28
sidebar_label: Datavault4dbt Snippets
title: Datavault4dbt Snippets
---

# DATAVAULT4DBT SNIPPETS

---

To make it easier for you to use Datavault4dbt while using Visual Studio Code as your IDE, we created an extension with a collection of useful snippets to scaffold your data vault models as well as pre-populate hash keys and model names according to our [naming conventions](https://www.scalefree.com/knowledge/webinars/data-vault-friday/data-vault-naming-conventions/).

### FEATURES

- templates all datavault4dbt entities
- supporting code blocks for repeated tasks like setting hash key and hashdiff
- prepopulates hash keys from model name

### HOW TO INSTALL

The simplest way to install the Datavault4dbt Snippets extension is directly from within the editor by navigating to the Extensions view. Once there, you can search for **datavault4dbt Snippets** and click the **Install** button on its page.

### HOW TO USE

Datavault4dbt snippets are prefixed with two underscores. Codeblocks created by these have several prepopulated fields that you can go through by tabbing through them. When creating a Stage, you can use the `__stg` snippet to create the skeleton for the stage macro that you can then tab through to fill out all steps necessary to build your model. Create a new hashkey by using the `__hk` block, add a multi-active satellite by using `__ma_config`, and many more.

#### SNIPPETS

| Category                | Type                              | Prefix |
|-------------------------|-----------------------------------|--------|
| **Datavault4dbt entities** |                                |        |
| Stage                   |                                   | `__stg`, `__stg_dv` |
| Hub                     | Standard                          | `__hub`, `__h` |
| Link                    | Standard                          | `__link`, `__l` |
| Link                    | Non-historized                    | `__nh_link`, `__nhl`, `__nl` |
| Satellite               | Standard v0                       | `__sat_v0`, `__v0` |
| Satellite               | Standard v1                       | `__sat_v1`, `__v1` |
| Satellite               | Non-historized                    | `__nh_sat` |
| Satellite               | Record-Tracking                   | `__rts` |
| Satellite               | Effectivity                       | `__es`, `__effsat`, `__eff_sat` |
| Satellite               | Multi-Active v0                   | `__msat_v0`, `__ms_v0` |
| Satellite               | Multi-Active v1                   | `__msat_v1`, `__ms_v1` |
| Reference               | Hub                               | `__ref_hub`, `__rh` |
| Reference               | Satellite v0                      | `__ref_sat_v0`, `__rs_v0` |
| Reference               | Satellite v1                      | `__ref_sat_v1`, `__rs_v1` |
| Reference               | Table                             | `__ref_table`, `__r` |
| PIT Table               |                                   | `__PIT`, `__pit` |
| Snapshot Control        | v0                                | `__snap_ctrl_v0`, `__sc_v0` |
| Snapshot Control        | v1                                | `__snap_ctrl_v1`, `__sc_v1` |
|                         |                                   |        |
| **Supporting Snippets** |                                   |        |
| Stage                   | Hash Key                          | `__hk`, `__hashkey` |
| Stage                   | Hash Diff                         | `__hd`, `__hashdiff` |
| Stage                   | Prejoined Column                  | `__prejoin` |
| Stage                   | Derived Column                    | `__stg_derived_column` |
| Stage                   | Missing Column                    | `__stg_missing_column` |
| Stage                   | Multi-active Config               | `__ma_config`, `__multi_active_config` |
| Raw Vault               | Source Model                      | `__src_model`, `__source_model` |
| Business Vault          | Snapshot Control Desired Duration | `__log_logic`, `__logarithmic_logic` |

**Enjoy!**