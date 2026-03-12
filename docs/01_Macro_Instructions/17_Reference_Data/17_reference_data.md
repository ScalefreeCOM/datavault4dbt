---
sidebar_position: 17
sidebar_label: Reference Data
title: Reference Data
---

# REFERENCE DATA

---

Datavault4dbt provides an advanced way of reference data storage. This includes:

- **reference Hub:** An entity that holds the reference key(s) for a reference object.
- **reference Satellites:** Satellites attached to a reference Hub that store the descriptive data and keep track of changes over time. Similar to regular Satellites, multiple satellites can be attached to one reference Hub.
- **reference Tables:** A presentation layer that re-unites a reference Hub with all its reference Satellites. Typically this would be done only virtually, but in some cases this object might be materialized as a table. Datavault4dbt allows users to historize these objects in three different ways; store only the latest descriptive data, capture all deltas, or apply a snapshot-based pattern.

## WHAT HAPPENED TO THE OLD REFERENCE TABLES?

The "classic" way to model reference table would typically only include one reference table, that holds both the reference keys and the descriptive data. It was optional to split attributes into separate reference satellites.

But after years of seeing reference data in action, we would now always recommend to directly split all descriptive reference data into separate satellites. The main advantage is to directly be ahead of future changing requirements regarding historization, privacy regulations or additional source systems.