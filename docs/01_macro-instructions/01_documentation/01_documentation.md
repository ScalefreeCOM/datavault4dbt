---
sidebar_position: 1
sidebar_label: Documentation
title: Documentation
---

# DOCUMENTATION

---

The following documentation sheds some light on the dbt-macros that have been developed by Scalefree to make your DataVault-Experience more comfortable! The documentation can be found by clicking the links in the sidebar on the right side. In the documentation, the macros and their parameters are explained and further exemplified.

## FUSION COMPATIBILITY 
Datavault4dbt is fully compatible with the new dbt Fusion engine! For more details, check here.

## INCLUDED MACROS 
- Staging Area (For Hashing, prejoins and ghost records)
- Hubs, Links & Satellites (allowing multiple deltas)
- Non-Historized Links and Satellites
- Multi-Active Satellites
- Effectivity and Record Tracking Satellites
- Reference Data Entities
- Virtualized End-Dating (in Satellites)
- PIT Tables
  - Hook for Cleaning up PITs
- Snapshot Control

## FEATURES 
With datavault4dbt you will get a lot of awesome features, including:

- A Data Vault 2.0 implementation congruent to the original Data Vault 2.0 definition by Dan Linstedt
- Ready for both Persistent Staging Areas and Transient Staging Areas, due to the allowance of multiple deltas in all macros, without loosing any intermediate changes- Enforcing standards in naming conventions by implementing global variables for technical columns
- A fully auditable solution for a Data Warehouse
- Creating a centralized, snapshot-based Business interface by using a centralized snapshot table supporting logarithmic logic
- A modern insert-only approach that avoids updating data
- Optimizing incremental loads by implementing a high-water-mark that also works for entities that are loaded from multiple sources
- A straight-forward, standardized approach to conduct agile datawarehouse development cycles

## REQUIREMENTS 
To use the macros efficiently, there are a few prerequisites you need to provide:

- Flat & Wide source data, available within your target database
- Load Date column that represents the arriving time in the source data storage
- Record Source column that gives information about where the source data is coming from (e.g. the file location inside a Data Lake)

## RESOURCES 
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://chat.getdbt.com/) on Slack for live discussions and support
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt`s development and best practices
- Find [dbt events](https://events.getdbt.com/) near you
- Check out the [Scalefree-Blog](https://www.scalefree.com/blog/)
  - [Data-Vault 2.0 with dbt #1](https://www.scalefree.com/blog/data-vault-2-0-with-dbt-1/)
  - [Data-Vault 2.0 with dbt #2](https://www.scalefree.com/blog/data-vault-2-0-with-dbt-2/)
  - [Using Multi-Active-Satellites #1](https://www.scalefree.com/scalefree-newsletter/using-multi-active-satellites-the-correct-way-1-2/)
  - [Using Multi-Active-Satellites #2](https://www.scalefree.com/scalefree-newsletter/using-multi-active-satellites-the-correct-way-2-2/)
  - [Non-Historized Links](https://www.scalefree.com/modeling/the-value-of-non-historized-links/)
  - [Bridge Tables](https://www.scalefree.com/scalefree-newsletter/bridge-tables-101/)
  - [PIT Tables](https://www.scalefree.com/scalefree-newsletter/point-in-time-tables-insurance/)
  - [Hash Keys in Data-Vault](https://www.scalefree.com/architecture/hash-keys-in-the-data-vault/)