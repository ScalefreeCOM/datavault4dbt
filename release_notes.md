# Version 1.1 

## Changes

- General
  - A new macro for 'hash_method' is added and replaces the call of the global variable 'datavault4dbt.hash'.

## New Features

### Reference Objects

We have added Data Vault constructs for storing reference data! That includes:

- Reference Hubs
- Reference Satellites (v0 & v1)
- Reference Tables

Check out the [new corresponding wiki page](https://github.com/ScalefreeCOM/datavault4dbt/wiki/Reference-Data) for more information!

### Stages (prejoins):

- General
  - If you want to materialize a stage incrementally (instead of views), a high water mark is now applied in the background. The first CTE filters the incoming data down to only those records that have a load date larger than the maximum existing load date in the target entity.
- Prejoins
   - Prejoins can now also refer to other dbt models instead of only sources. Use `ref_model: <dbt_model>` instead of `src_name: <source_name>` & `src_table: <source_table>`
   - 'this_col_name' and 'ref_col_name' can now use multiple columsn to populate the join condition. The operator to combine these columns can be set by specifying the parameter 'operator'. Default is 'AND'.
- Derived columns
   - Derived columns do not have to be renamed anymore. If you want to override an input column by applying hard rules, the input source column will no longer be selected. This applies when a derived column is named similar as a source column. 

# Contributors
- @bschlottfeldt for adding the hash_method() macro and adapting all other features to Exasol!