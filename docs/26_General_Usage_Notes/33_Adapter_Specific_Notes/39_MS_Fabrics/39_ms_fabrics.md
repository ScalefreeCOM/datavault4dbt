---
sidebar_position: 39

sidebar_label: MS Fabrics
title: MS Fabrics
---

# MS FABRICS

---

## LIMITATION OF AMOUNT FOR COLUMNS IN HASHDIFF

Due to the max amount of arguments of the `CONCAT_WS()`-function the current (v1.9.11) maximum amount of columns for a singular hashdiff on MS Fabric is 253.

## ADAPTER SPECIFIC VARIABLE
The variable `datavault4dbt.set_casing` can be used to force all column names to be uppercased or lowercased. Allowed values for the variable are `upper`, `uppercase`, `lower`, `lowercase`.