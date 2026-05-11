---
sidebar_position: 16
sidebar_label: Non-Historized Satellite
title: Non-Historized Satellite
---

# NON-HISTORIZED SATELLITE

---

This macro creates a non-historized satellite that should be materialized as an incremental table. It should be applied `on top` of the staging layer, and is either connected to a Hub or a Link. Besides the missing hashdiff, a non-historized satellite applies the same loading logic as a regular version 0 satellite. Each satellite can only be loaded by one source model, since we typically recommend a satellite split by source system.

Features:

- High-Performance loading of non-historized satellite data

### REQUIRED PARAMETERS

| Parameters | Data Type | Required | Default Value | Explanation |
| :--- | :--- | :--- | :--- | :--- |
| parent_hashkey | string | mandatory | - | Name of the hashkey column inside the stage of the object that this satellite is attached to. |
| src_payload | string \| list of strings | mandatory | - | A list of all the descriptive attributes that should be included in this satellite. |
| source_model | string | mandatory | - | Name of the underlying staging model, must be available inside dbt as a model. |

### OPTIONAL PARAMETERS

| Parameters | Data Type | Required | Default Value | Explanation |
| :--- | :--- | :--- | :--- | :--- |
| source_is_single_batch | boolean | optional | False | See below for explanation. |
| src_ldts | string | optional | datavault4dbt.ldts_alias | Name of the ldts column inside the source models. Needs to use the same column name as defined as alias inside the staging model. |
| src_rsrc | string | optional | datavault4dbt.rsrc_alias | Name of the rsrc column inside the source models. Needs to use the same column name as defined as alias inside the staging model. |
| additional_columns | string \| list of strings | optional | none | Column or list of columns that will additionally be added to the non-historized satellite. |

## EXAMPLE 1

```jinja
{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
parent_hashkey: 'hk_creditcard_transactions_nl'
src_payload:
    - invoice_address
    - vendor_name
source_model: 'stage_creditcard_transactions'
{%- endset -%}    

{{ datavault4dbt.nh_sat(yaml_metadata=yaml_metadata) }}   
```

### DESCRIPTION

With this example, a non-historized Satellite is created. Under normal circumstances, all descriptive attributes of a transaction are integrated into a non-historized link. In this example, there are some attributes that contain personal identifiable information (PII), which has to be treated differently than the other attributes. For that split, the non-historized satellite comes into play: all PII-attributes are integrated into a non-historized satellite, which is attached to the corresponding non-historized link.

- **parent_hashkey**:
  - __hk_creditcard_transactions_nl__: The satellite would be attached to the non-historized link creditcard_transaction, which has the column `hk_creditcard_transactions_nl` as a hashkey column.
- **src_payload**:
  - __[`invoice_address` ,`vendor_name`]__: This satellite would hold the columns `invoice_address` and `vendor_name` out of the underlying staging area.
- **source_model**:
  - __stage_creditcard_transactions__: This satellite is loaded out of the stage for creditcard_transactions

### DISABLING DEDUPLICATION OF THE SOURCE DATA

To safely disable the deduplication, you have to verify that the underlying staging model of the NH Sat only holds one row per Link Hashkey.

We highly recommend setting up a uniqueness test within a yml file, covering the hashkey column of the staging model.

Once this is guaranteed, you just add the following parameter to your macro call:

```jinja
source_is_single_batch=true
```

If this is set to true, this __QUALIFY__ statement:

```jinja
earliest_hk_over_all_sources AS (
{# Deduplicate the unionized records again to only insert the earliest one. #}

    SELECT
        lcte.*
    FROM {{ ns.last_cte }} AS lcte

    QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ link_hashkey }} ORDER BY {{ src_ldts }}) = 1

    {%- set ns.last_cte = 'earliest_hk_over_all_sources' -%}

),
```

will not be activated!

To continuously ensure data validity within your NH Sat, we also recommend setting up a uniqueness test across the Link Hashkey on your NH Sat.