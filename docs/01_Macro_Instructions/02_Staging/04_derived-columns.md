---
sidebar_position: 4
sidebar_label: Derived Columns
title: Derived Columns
---

# DERIVED COLUMNS

---

A stage model is capable of defining Derived Columns. As the name indicates, this allows users to apply Hard Rules for transformation.

The metadata structure within a stage model looks like this:

```jinja
derived_columns: 
    <col_alias_1>:
        value: <expression_1>
        datatype: <datatype_1>
        src_cols_required: <src_col_1>
    <col_alias_2>:
        value: <expression_2>
        datatype: <datatype_2>
        src_cols_required:
            - <src_col_2>
            - <src_col_3>
```

Depending on how `col_alias` and `src_col` are called, two different behaviors can be achieved:

### OVERWRITING EXISTING COLUMNS
When `col_alias` equals `src_col`, the original input column will be overwritten with the transformation configured in `expression`.
**Use with caution!**

### ADDING NEW COLUMNS
When `col_alias` deviates from the`src_col`, the transformation will be added as a new column.

## EXPRESSIONS
An expression is defined under the key `value` and can basically do three different things:

- **Inserting a static string** that will be the same across all rows. Needs to begin with `!` followed by the string.
- **Renaming a column.** Expression would just be the name of another column.
- **Applying SQL.** The Expression needs to hold valid SQL, typically based on one or multiple columns. Note: All used columns should be listed under `src_cols_required`.

## DATATYPES
Defining a datatype of the expressions is mandatory to properly generate Ghost Records. Please note, that setting a datatype does not automatically cast the expression to this datatype. You manually have to ensure that your expression matches the datatype defined.

It must only be set for SQL expressions. For static strings, it will be set to STRING (and the database correspondents) automatically. For column renaming, datatype will be set to the datatype of the input column.

## REQUIRED SOURCE COLUMNS
The parameter `src_cols_required` is only required when the Stage model is configured to not include the source columns by setting the parameter `include_source_columns` to false.

If this is the case, you have to list all columns used within the SQL expressions under the parameter `src_cols_required`. This information is required to properly generate the model SQL.