---
sidebar_position: 26
sidebar_label: Fusion Compatibility
title: Fusion Compatibility
---

# FUSION COMPATIBILITY

---

datavault4dbt is fully compatible with dbt Fusion!

As of datavault4dbt v.1.14.0, some SQL components are still considered invalid by the Fusion engine, which would lead to errors when executing datavault4dbt with dbt Fusion.

To avoid this, dbt introduced the [`static_analysis` config](https://docs.getdbt.com/reference/resource-configs/static-analysis) which needs to be set to `off` for all affected dbt models. To increase usability, datavault4dbt automatically sets this config to `off` where it’s required - enabling out-of-the-box compatibility. If you want to disable this behavior, set the global variable `datavault4dbt.enable_static_analysis_overwrite` to `false`.

The following table shows which macros are causing issues on each database right now, and this information is used in the [background macro `get_static_analysis_config()`](https://github.com/ScalefreeCOM/datavault4dbt/blob/main/macros/supporting/fusion_static_analysis.sql).

| Macro            | Snowflake | BigQuery | Databricks | Redshift |
|------------------|-----------|----------|------------|----------|
| stage            | ✅       | ⚠️       | ⚠️         | ✅      |
| hub              | ✅       | ✅       | ✅         | ✅      |
| link             | ✅       | ✅       | ✅         | ✅      |
| sat_v0           | ✅       | ✅       | ✅         | ✅      |
| sat_v1           | ✅       | ✅       | ⚠️         | ✅      |
| ma_sat_v0        | ✅       | ✅       | ✅         | ✅      |
| ma_sat_v1        | ✅       | ✅       | ⚠️         | ⚠️      |
| nh_link          | ✅       | ✅       | ✅         | ✅      |
| nh_sat           | ✅       | ✅       | ✅         | ✅      |
| eff_sat_v0       | ✅       | ✅       | ✅         | ✅      |
| rec_track_sat    | ✅       | ✅       | ✅         | ✅      |
| ref_hub          | ✅       | ✅       | ✅         | ✅      |
| ref_sat_v0       | ✅       | ✅       | ✅         | ✅      |
| ref_sat_v1       | ✅       | ✅       | ⚠️         | ✅      |
| ref_table        | ✅       | ✅       | ✅         | ✅      |
| control_snap_v0  | ✅       | ⚠️       | ✅         | ✅      |
| control_snap_v1  | ✅       | ✅       | ⚠️         | ✅      |
| pit              | ✅       | ✅       | ✅         | ✅      |

## DATABASE SPECIFIC LIMITATIONS

In the following, we listed all kinds of known limitations of the Fusion engine that we encountered while working with it. If you have any kind of errors, this might help!

### SNOWFLAKE

- Seeds with TIMESTAMP_TZ are not working. [Github Issue](https://github.com/dbt-labs/dbt-fusion/issues/895)

### BIGQUERY

- Stage models must not select from seeds. [Github Issue](https://github.com/dbt-labs/dbt-fusion/issues/1102)
- Seeds can’t have TIMESTAMP columns. [Github Issue](https://github.com/dbt-labs/dbt-fusion/issues/999)

### DATABRICKS

- Big general Fusion issues with incremental models when hive metastore is used. dbt advises [to switch to Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/enable-workspaces#enable-your-workspace-for-unity-catalog).

### REDSHIFT

- Problems with view models which select from other view models. [Github Issue](https://github.com/dbt-labs/dbt-fusion/issues/1118)