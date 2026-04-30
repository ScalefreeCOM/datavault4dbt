---
sidebar_position: 35
sidebar_label: Exasol
title: Exasol
---

# EXASOL

---

For configuring your dbt project with Exasol adapter you are going to need dbt-core installed in a Docker container with dbt-exasol. I would recommend setting up a Docker Compose container, with one container holding the connection to Exasol database and the other with your development environment setup.

In your dbt project profiles.yml follow the instructions on this dbt page to set up your connection to Exasol database: [Exasol Setup](https://docs.getdbt.com/docs/core/connect-data-platform/exasol-setup)

## Usage Notes

- When using ORDER BY clause in your SQL query, the columns used to order need to also be in the Select clause.
- Avoid small data inserts, Exasol performs better multi-row inserts or Bulk Inserts than single row inserts.
- Exasol doesn’t support UNIQUE constraint
- Primary key, foreign key and not-null constraints are ENFORCED.
- Use exact datatypes, and avoid setting columns with large length sizes if not needed.
- Use identical types for same content: Avoid joining columns of different data types. For example, do not join an INT column with a VARCHAR column containing digits.
- In Exasol empty strings are treated as NULL
- IMPORT vs. INSERT : We recommend you use the IMPORT statement over INSERT as it performs better than the INSERT statement. If you are loading smaller data (say about 100 rows), using INSERT statement might be faster. However, for larger amounts of data (10,000 plus rows), IMPORT is the fastest and the best way to load data from external sources. Queries and inserts/imports on partitioned tables will run more efficiently.

## DATA VAULT PERFORMANCE BEST PRACTICES ON EXASOL

- Hashes have a special datatype: HASHTYPE(16 BYTE). Hashes mean better performance for your analyses and reporting thanks to improved joins between tables and schemas. These joins use hashes and run approximately four times faster than joins based on strings. In datavault4dbt HASHTYPE is already the dafult datatype for Hashkeys in Exasol, it is recommended to keep like this.
- Exasol allows partitioning and distribution of tables. **A column should be set as a distribution key if it is frequently used in JOIN operations**. We recommend altering Hub tables to be distributed by its Hashkey and Satellite tables to be distributed by the Hashkey of their parent, since this will be frequently used in joins with Hubs/Links. **Don’t**: Distribute on columns used in WHERE clauses (filters), which leads to global joins and disables the MPP functionality, both causing poor performance in Exasol.
- If tables are too large respectively too many to fit completely into the node’s memory, partitioning large tables can help improve performance. In contrast to distribution, **partitioning should be done on columns that are used for filtering**.