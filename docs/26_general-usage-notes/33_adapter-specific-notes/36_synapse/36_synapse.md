---
sidebar_position: 36
sidebar_label: Synapse
title: Synapse
---

# SYNAPSE

---

## HANDLING NVARCHAR MAX COLUMNS IN SYNAPSE

When working with Synapse, it’s important to address how the system manages NVARCHAR MAX columns, as this aspect significantly impacts the utilization of column store indexes. Unlike some other platforms, Synapse cannot use NVARCHAR MAX columns directly as an index within column store indexes. This limitation arises due to the way Synapse handles large object (LOB) data, which includes NVARCHAR MAX types.

### KEY CONSIDERATIONS:

- **Source System Data Types:** Often, source systems define columns as NVARCHAR(MAX). While this allows for flexibility in storing varying lengths of character data, it poses challenges for indexing and performance optimization in Synapse.
- **Column Store Index Compatibility:** To leverage the full capabilities of Synapse’s column store indexes, it’s crucial to avoid using NVARCHAR MAX columns directly. These columns are not supported for indexing due to their potential size and storage requirements.

### RECOMMENDED WORKAROUNDS:

- **Use Alternative Data Types:** Where possible, consider using specific NVARCHAR lengths instead of NVARCHAR(MAX). Defining a maximum length that suits your data requirements can ensure compatibility with column store indexes while maintaining efficient data storage and access.
- **Convert Columns in Staging:** For cases where NVARCHAR(MAX) columns are unavoidable, perform a conversion in a staging area. This involves creating derived columns with a defined NVARCHAR length that is suitable for your data but within the limits that Synapse supports for indexing.

## HASHTYPES IN SYNAPSE

When using the datavault4dbt package with Synapse, you must adjust the data types for hash keys due to compatibility issues. The default hash type configuration in the package uses string or VARCHAR, which is not supported by Synapse in our implementation.

### KEY CONSIDERATIONS:

- **Default Configuration:** Typically, hash keys are set as string or VARCHAR in the package.
- **Synapse Compatibility:** These data types are not supported for hash keys in Synapse within our package.

### RECOMMENDED CONFIGURATION:

- **Switch to binary(16):** For successful deployment on Synapse, change the hash key data type (datavault4dbt.hash_datatype) in your dbt project yaml configuration to binary(16). This ensures compatibility with the Synapse environment.

### INDEXING

Synapse wants to index your keys by default. To make it work out of the box with our package you need to configure that in the project yml file.

### RECOMMENDED CONFIGURATION:

- **Set Index to HEAP:** In your project.yml under your schemas, set the index to heap. This is tested and working. For that add the Index key right under datavault4dbt in the hierarchy. Like this: `models: datavault4dbt: index: HEAP`. Make sure that the indent is correct. Other index options might be available, use them as needed and check the synapse documentation for further information.