---
sidebar_position: 3
sidebar_label: Prejoining
title: Prejoining
---

# PREJOINING

---

Prejoins are described as dictionaries with the following keys:

| Key                 | Data Type                 | Explanation |
|---------------------|---------------------------|-------------|
| extract_columns     | string \| list of strings | Single column or list of columns that will be extracted from the prejoin. If no alias is given the columns will have the same name as in the target-relation. |
| aliases (optional)  | string \| list of strings | Optionally, aliases for the `extract_columns` can be defined. If given, it needs to have the same number of columns as in `extract_columns`, otherwise a compilation error will be thrown. |
| this_column_name    | string \| list of strings | Specifies one or more columns within the source model of this stage, that should be used as the JOIN condition of the Prejoin. Can be multiple columns, but must match the number of columns defined in `ref_column_name`. |
| ref_column_name     | string \| list of strings | Specifies one or more columns within the referenced object of this prejoin, that should be used as the JOIN condition of the Prejoin. Can be multiple columns, but must match the number of columns defined in `this_column_name`. |
| ref_model           | string                    | Name of the other dbt model that should be referred. Either this, or the parameters `src_name` and `src_table` must be defined. |
| src_name            | string                    | If a dbt source should be prejoined, use this parameter to set the name of the source, as defined in the sources section of a yml file. Must be used together with `src_table`, and instead of `ref_model`. |
| src_table           | string                    | If a dbt source should be prejoined, use this parameter to set the table of the source `src_name`, as defined in the sources section of a yml file. Must be used together with `src_name`, and instead of `ref_model`. |
| operator (optional) | string                    | Only used when multiple columns are defined for `this_col_name` and `ref_col_name`. Influences which logical operator is used to combine multiple JOIN conditions. Default is `AND`, use only if other operator is desired. |
| join_type (optional)| string                    | Can be used to determine the join type (left, right, inner, etc...). Whatever string is passed as `join_type` parameter will be inserted before the `join` keyword in the compiled staging-model. Defaults to `left`. New in v1.9.11. |

Prejoining is used to enrich source data by attributes from other database objects. In general, it should only be used when the source data does not hold the Business Key, but the technical Key of an object.

## CONFIGURING PREJOINS

Within one Stage model, users can setup extraction of one to many columns from other database objects. Per prejoin one dictionary is defined:

```jinja
prejoined_columns:
    - extract_columns: 
         - <name_of_column_to_be_selected>
      aliases:
         - <extracted_column_alias>
      ref_model: <name_of_other_dbt_model>
      this_column_name: <name_of_col_in_this_object>
      ref_column_name: <name_of_col_in_ref_object>
    - extract_columns: 
         - <name_of_other_column_to_be_selected>
      aliases:
         - <other_extracted_column_alias>
      src_name: <name_of_dbt_source>
      src_table: <name_of_table_within_dbt_source>
      this_column_name: 
          - <name_of_col_1_in_this_object>
          - <name_of_col_2_in_this_object>
      ref_column_name: 
          - <name_of_col_1_in_ref_object>
          - <name_of_col_2_in_ref_object>
```

Example of a definition of prejoined_columns parameter in an example stage:

```jinja
prejoined_columns:
    - extract_columns: 
         - id
      aliases:
         - businessid
      ref_model: business_raw
      this_column_name: ContractId
      ref_column_name: ContractId
    - extract_columns: 
         - contractnumber
         - contractkey
      aliases:
         - contractnumber_pj
         - contractkey_pj
      src_name: stg_prod
      src_table: contract
      this_column_name: 
          - contract_id
          - other_column
      ref_column_name: 
          - id
          - other_column
      operator: OR 
```