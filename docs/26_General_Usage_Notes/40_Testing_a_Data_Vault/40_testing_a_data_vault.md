---
sidebar_position: 40
sidebar_label: Testing a Data Vault
title: Testing a Data Vault
---

# TESTING A DATA VAULT

---

## INTRODUCTION

In general, you should always test your data, no matter what modeling approach you follow, no matter which tool you use. The combination of dbt and Data Vault 2.0 makes it very easy to test the data early on, in the Raw Data Vault.

Testing your data typically consists out of two parts, **technical** and **business tests**. Business tests are closely tied to the data concepts and contents of your organization and are not to be discussed here. This section **focuses on technical tests of a Raw Data Vault**, and how to implement them in dbt.

## HARD CONSTRAINTS VS. SOFT CONSTRAINTS

### HARD CONSTRAINTS

A Hard constraint is a constraint enforced on one or multiple columns, directly on the database. When inserting into a column with a defined constraint, the database will check, whether the inserted data violates the constraint or not. If it would violate it, the data would not be inserted. A couple of examples for database constraints:

Primary Key constraint on Hub or Link Hashkeys, within Hubs or Links
Primary Key constraint on Hashkey + load_date within a Satellite
Foreign Key constraint between Hub Hashkey in Link, and Hub Hashkey in Hub
Foreign Key constraint between Hashkey in Satellite, and Hashkey in Parent entity
Not Null constraint on specific columns in RDV or BDV

### SOFT CONSTRAINTS

Compared to Hard Constraints, Soft Constraints do not stop data, that violates a constraint, to be loaded into the target entity. Instead, a warning is shown. This ensures, that no data is lost.

Soft Constraints can be implemented as tests, that are applied on your data after loading a table.

### CONSTRAINTS IN DATA VAULT 2.0

In Data Vault 2.0, hard-constraints on the database are typically seen as a bad practice, since they would lead to the possibility, that raw data is not captured, if it would violate a constraint. You always want to catch the good, bad, and ugly data, no matter if it violates some assumptions. If, for some reason, you really need to use hard database constraints, you need to be clear about the risk of losing data, and it is recommended to only use hard constraints if Error Marts are implemented.

As a general recommendation, Soft Constraints should be the first choice, when using Data Vault 2.0

## SOFT CONSTRAINTS IN DBT

As said before, a soft constraint can be implemented as a test. That makes soft constraints a perfect use case for dbt tests. Every constraint can be defined as a test on a column, or on multiple columns, inside .yml files.

## TESTING THE RAW DATA VAULT IN DBT

To start basic testing of a Raw Data Vault, lets turn all Primary Key and Foreign Key assumptions of Data Vault 2.0 into dbt tests. This creates a list of tests per entity type:

| Entity Type                   | Scope / Column                           | Test |
|-------------------------------|------------------------------------------|------|
| Hub                           | Hashkey                                  | not_null |
| Hub                           | Hashkey                                  | unique |
| Link                          | Link Hashkey                             | not_null |
| Link                          | Link Hashkey                             | unique |
| Link                          | Foreign Hashkeys                         | relationship to all connected Hubs |
| Satellite (v0)                | Hashkey + LoadDate                       | unique_combination_of_columns |
| Satellite (v0)                | Hashkey                                  | relationship to parent Hub/Link |
| Non-Historized Link           | Link Hashkey                             | not_null |
| Non-Historized Link           | Link Hashkey                             | unique |
| Non-Historized Link           | Foreign Hashkeys                         | relationship to all connected Hubs |
| Non-Historized Satellite (v0) | Hashkey                                  | not_null |
| Non-Historized Satellite (v0) | Hashkey                                  | unique |
| Non-Historized Satellite (v0) | Hashkey                                  | relationship to parent NH-Link |
| Multi Active Satellite (v0)   | Hashkey + LoadDate + Multi Active Key(s) | unique_combination_of_columns |
| Multi Active Satellite (v0)   | Hashkey                                  | relationship to parent Hub/Link |
| Reference Hub                 | Reference Key(s)                         | unique |
| Reference Hub                 | Reference Key(s)                         | not_null |
| Reference Satellite (v0)      | Reference Key(s) + LoadDate              | unique_combination_of_columns |
| Reference Satellite (v0)      | Reference Key(s)                         | relationship to parent Hub/Link |
| Record Tracking Satellite     | Hashkey + LoadDate                       | unique_combination_of_columns |
| Record Tracking Satellite     | Hashkey                                  | relationship to parent Hub/Link |