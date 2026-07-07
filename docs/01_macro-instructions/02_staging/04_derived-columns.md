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
    <col_alias_3>:
        value: <expression_3>
        datatype: <datatype_3>
        src_cols_required: <src_col_3>
        overwrite_src_cols: true   # optional — drops src_col_3 from the output
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
- **Applying SQL.** The Expression needs to hold valid SQL, typically based on one or multiple columns. Note: All used columns should be listed under `src_cols_required`. Since SQL should not be escaped, the background macro needs to identify SQL Code (or numerical values). It does this by checking for the use of paranthesis `()`. So **if you experience quoting issues with your derived column, try wrapping it in paranthesis** and check if this solves it.

## DATATYPES
Defining a datatype of the expressions is mandatory to properly generate Ghost Records. Please note, that **setting a datatype does not automatically cast the expression to this datatype**. You manually have to ensure that your expression matches the datatype defined, optionally by including a `CAST` or `CONVERT` to your value. 

It must only be set for SQL expressions. For static strings, it will be set to STRING (and the database correspondents) automatically. For column renaming, datatype will be set to the datatype of the input column.

## REQUIRED SOURCE COLUMNS
The parameter `src_cols_required` is only required when the Stage model is configured to not include the source columns by setting the parameter `include_source_columns` to false.

If this is the case, you have to list all columns used within the SQL expressions under the parameter `src_cols_required`. This information is required to properly generate the model SQL.

## OVERWRITE SOURCE COLUMNS

The optional boolean parameter `overwrite_src_cols` can be set per derived column. When set to `true`, the source columns listed under `src_cols_required` are **excluded from the staging CTE output**, so only the derived alias column appears — preventing duplicate columns.

This is particularly useful when **renaming columns whose original name cannot be referenced downstream**, for example columns containing special characters (e.g. German umlauts like `ä`, `ö`, `ü`) on platforms such as Databricks. Without `overwrite_src_cols: true`, both the original and the renamed column would appear side by side in the output.

```jinja
derived_columns:
    account_name_clean:
        value: Kontonäme
        datatype: STRING
        src_cols_required: Kontonäme
        overwrite_src_cols: true
```

In this example, `Kontonäme` is excluded from the staging CTE and only `account_name_clean` appears in the output. The parameter defaults to `false` when omitted.
