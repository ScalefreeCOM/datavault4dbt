---
sidebar_position: 37
sidebar_label: Oracle
title: Oracle
---

# ORACLE

---

## HANDLING VARCHAR2 AND EXTENDED STRING SIZE IN ORACLE

When working with Oracle, managing the maximum string size is a crucial aspect to consider, particularly when dealing with hash diffs and large data processing workflows. Oracle offers two settings for maximum string size - **Standard** and **Extended** - and choosing the correct configuration directly impacts compatibility and performance.

### KEY CONSIDERATIONS:

1. **Max String Size**: By default, Oracle uses **Standard** mode, where VARCHAR2 can only store up to 4,000 bytes. However, when working with Datavault4dbt or other automation tools requiring larger strings for hash diffs, it’s essential to switch to **Extended** mode. This allows VARCHAR2 to store up to 32,767 bytes, ensuring that large hash diff columns can be processed effectively.

2. **Hashdiffs Compatibility**: Hash diff fields, which typically require more space, can exceed the limits imposed by the Standard mode. If the max_string_size is not set to **Extended**, these hash diffs may cause errors during data ingestion or transformation in Oracle.

### RECOMMENDED CONFIGURATION:

**Set Max_String_Size to Extended:** To enable support for larger strings, set the following system parameter:

`ALTER SYSTEM SET max_string_size = EXTENDED SCOPE=BOTH;`

This configuration ensures that VARCHAR2 fields are large enough to store the hash diffs produced by the Datavault4dbt package and other similar.

A variable `datavault4dbt.oracle_varchar_size` (defaulting to 32767) was added in datavault4dbt v1.12.4 to allow to change the varchar size of the hash standardization.