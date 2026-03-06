---
sidebar_position: 38
sidebar_label: Databricks
title: Databricks
---

# DATABRICKS

---

This article focuses on Databricks-specific considerations for users of the datavault4dbt package, specifically regarding the calculation of the multi-active hashdiff when working with Databricks. In Databricks, there are some limitations with string aggregation functions, which necessitate a customized approach to ensure deterministic results.

## PROBLEM: LACK OF LIST_AGG FUNCTION IN DATABRICKS

Databricks does not support the `LIST_AGG` function, which is commonly used in other platforms to aggregate string values when calculating the multi-active hashdiff. The multi-active hashdiff is a unique hash calculation needed when there are multiple active records for a specific business key at the same time, and you need to ensure deterministic results.

In Databricks, the following functions must be used to replicate the behavior of `LIST_AGG`:

- `ARRAY_AGG()`: Collects values into an array.

- `SORT_ARRAY()`: Sorts the array to ensure a consistent order.

- `ARRAY_JOIN()`: Joins the array elements into a string.

### EXAMPLE:

`ARRAY_JOIN(SORT_ARRAY(ARRAY_AGG(column_name)), ',')`

While this combination of functions mimics the functionality of `LIST_AGG`, it introduces a significant problem: you cannot directly specify an order by clause inside the `ARRAY_AGG()` function. This prevents sorting by the multi-active key, which is necessary to ensure deterministic hashdiff calculations.

## WHY ORDER BY MULTI-ACTIVE KEY IS IMPORTANT

To achieve consistent and reproducible results in hashdiff calculations, it is crucial to aggregate string values in a specific, predictable order. Without the ability to order by the multi-active key, the calculation may produce different hash values across executions for the same input data, which would break the integrity of the data vault model.

## SOLUTION: USING A DERIVED COLUMN WITH ROW_NUMBER

To address this issue, we recommend introducing a derived column using the `ROW_NUMBER()` function to create a deterministic ordering within partitions based on the multi-active key. This ensures that string values are aggregated in a consistent order, even when working within the limitations of Databricks.

### Steps:

#### 1. CREATE A DERIVED COLUMN WITH ROW_NUMBER()

The first step is to generate a row number within each partition of records that share the same single_source_hk (hash key for the source data). This ensures that each record in a group of multi-active records gets a unique and deterministic row number based on the multi-active key.

`ROW_NUMBER() OVER (PARTITION BY single_source_hk ORDER BY multi_active_key) AS rn`

#### 2. ADD THE DERIVED COLUMN TO THE HASHDIFF CALCULATION

In the multi-active hashdiff calculation, include the rn (row number) as the first column. This will ensure that the records are consistently ordered based on the multi-active key, even when using the `ARRAY_AGG`, `SORT_ARRAY`, and `ARRAY_JOIN` combination.

## ADAPTER SPECIFIC VARIABLE

The variable `datavault4dbt.set_casing` can be used to force all column names to be uppercased or lowercased. Allowed values for the variable are `upper`, `uppercase`, `lower`, `lowercase`.