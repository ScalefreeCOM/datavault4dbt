---
sidebar_position: 2
sidebar_label: Staging
title: Staging
---

# STAGING

---

This macro creates the staging layer for the Data Vault model. This layer is mainly for hashing, and additionally gives the option to create derived columns, conduct prejoins and add NULL values for missing columns. Always create one stage per source table that you want to add to the Data Vault model. The staging layer is not to harmonize data. That will be done in the later layers.

## REQUIRED PARAMETERS

| Parameter      | Data Type             | Required  | Default Value     | Explanation |
| -------------- | --------------------- | --------- | ----------------- | ----------- |
| `ldts`         | string                | mandatory | current timestamp | Name of the column inside the source data, that holds information about the Load Date Timestamp. Can also be a SQL expression. If no ldts is passed, the current_timestamp-makro of datavault4dbt will be used to pass a value in the resulting staging table.
| `rsrc`         | string                | mandatory | –                 | Name of the column inside the source data, that holds information about the Record Source. Can also be a SQL expression or a static string. A static string must begin with a `!`.
| `source_model` | string \| dictionary  | mandatory | –                 | Can be just a string holding the name of the refered dbt model to use as a source. But if the `source` functionality inside the .yml file is used, it must be a dictionary with `source_name`: `source_table`.

---

## OPTIONAL PARAMETERS

| Parameter | Data Type | Required | Default Value | Explanation |
|-----------|-----------|----------|---------------|-------------|
| `include_source_columns` | boolean | important | True | Defines if all columns from the refered source table should be included in the result table, or if only the added columns should be part of the result table. By default the source columns should be included.
| `hashed_columns` | dictionary | important | None | Defines the names and input for all hashkeys and hashdiffs to create. The key of each hash column is the name of the hash column. The value for Hashkeys is a list of input Business Keys, for Hashdiffs another dictionary with the keys `is_hashdiff:true`, `columns: `. <br /><br />  Optionally you can set the key `use_trim` to either true or false to override the global value of datavault4dbt.hashdiff_use_trim(defaulting to true). This option configures whether the hashdiff-input-columns are individually wrapped by TRIM()-functions. <br /><br />  Optionally you can set a key `use_rtrim:true` if you want the Hashdiff to be trimmed on the right side (This feature was available for Snowflake in the versions from v1.1.4 up to v1.2.2. It will become available for all adapters from v2.0.0 again.) |
| `derived_columns` | dictionary | important | None | Learn more about Derived Columns here. |
| `sequence` | string | optional | None | Name of the column inside the source data, that holds a sequence number that was generated during the data source extraction process. Optional and not required. |
| `prejoined_columns` | dictionary | important | None | Learn more about Prejoining here. |
| `missing_columns` | dictionary | optional | None | If the schema of the source changes over time and columns are disappearing, this parameter gives you the option to create additional columns holding NULL values, that replace columns that were previously there. By this procedure, hashdiff calculations and satellite payloads wont break. The dictionary holds the names of those columns as keys, and the SQL datatypes of these columns as values. |
| `multi_active_config` | dictionary | important | None | If the source data holds multi-active data, define here the column(s) holding the multi-active key and the main hashkey column. If the source data is multi-active but has no natural multi-active key, create one using the row_number SQL function (or similar) one layer before. Then insert the name of that artificial column into the multi-active-key parameter. The combination of the multi-active key(s), the main-hashkey and the ldts column should be unique in the final result satellite. If not set, the stage will be treated as a single-active stage. |
| `enable_ghost_records` | boolean | optional | True | This parameter makes it possible to disable the creation of ghost records. It is not possible to disable only the error values or unknown values, only both at the same time. |

---

## EXAMPLE 1

```jinja
{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: 'source_account'
ldts: 'edwLoadDate'
rsrc: 'edwRecordSource'
hashed_columns: 
    hk_account_h:
        - account_number
        - account_key
    hd_account_s:
        is_hashdiff: true
        columns:
            - name
            - address
            - phone
            - email
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

- **source_model**: The source model that you want to use for the stage is available as another dbt model with the name `source_account`.
- **ldts**: Uses the column called `edwLoadDate` as it is from the source model.
- **rsrc**: Uses the column called `edwRecordSource` as it is from the source model.
- **hashed_columns**:
  - **hk_account_h**: A hashkey called `hk_account_h` is defined, that is calculated out of the two business keys `account_number` and `account_key`
  - **hd_account_s**: A hashdiff called `hd_account_s` is calculated out of the descriptive attributes `name`, `address`, `phone`, and `email`.

---

## EXAMPLE 2

```jinja
{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: 'source_account'
ldts: 'edwLoadDate'
rsrc: 'edwRecordSource'
hashed_columns: 
    hk_account_h:
        - account_number
        - account_key
    hd_account_s:
        is_hashdiff: true
        columns:
            - name
            - address
            - phone
            - email
derived_columns:
    conversion_duration:
        value: 'TIMESTAMP_DIFF(conversion_date, created_date, DAY)'
        datatype: 'INT64'
    country_isocode:
        value: '!GER'
        datatype: 'STRING'
    account_name:
        value: 'name'
        datatype: 'String'
prejoined_columns:
    contractnumber:
        src_name: 'source_data'
        src_table: 'contract'
        bk: 'contractnumber'
        this_column_name: 'ContractId'
        ref_column_name: 'Id'
    master_account_key:
        src_name: 'source_data'
        src_table: 'account'
        bk: 'account_key'
        this_column_name: 'master_account_id'
        ref_column_name: 'Id'
missing_columns:
    legacy_account_uuid: 'INT64'
    shipping_address: 'STRING'
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

- **source_model**: The source model that you want to use for the stage is available as another dbt model with the name `source_account`.
- **ldts**: Uses the column called `edwLoadDate` as it is from the source model.
- **rsrc**: Uses the column called `edwRecordSource` as it is from the source model.
- **hashed_columns**:
  - **hk_account_h**: A hashkey called `hk_account_h` is defined, that is calculated out of the two business keys `account_number` and `account_key`
  - **hd_account_s**: A hashdiff called `hd_account_s` is calculated out of the descriptive attributes `name`, `address`, `phone`, and `email`.
- **derived_columns**:
  - **conversion_duration**: The column `conversion_duration` calculates the amount of days between two columns available inside the source data.
  - **country_isocode**: The column `country_isocode` inserts the static string `GER` for all rows.
  - **account_name**: The column `account_name` duplicates an already existing column and gives it another name.
- **prejoined_columns**:
  - **contractnumber**: Creates a column called `contractnumber` that holds values of the column with the same name (specified in `bk`) from the source table `contract` in the source `source_data` by joining on `this.ContractId = contract.Id`. In this case the prejoined column alias equals the name of the original business key column, which should be the case for most prejoins. But sometimes the same object is prejoined multiple times or a self-prejoin happens, and then you would have to rename the final columns to not have duplicate column names. That behaviour is seen in the next prejoined column.
  - **master_account_key**: The column `master_account_key` holds values of the column `account_key` inside the source table `account`. If this prejoin is done inside account, we would now have a self-prejoin ON `account.master_account_id = account.Id`. Because the table `account` already has a column `account_key`, we rename the prejoined column to `master_account_key`.
- **missing_columns**: Two additional columns are added to the source table holding NULL values. The column `legacy_account_uuid` will have the datatype `INT64` and the column `shipping_address` will have the datatype `STRING`.

---

## EXAMPLE 3

```jinja
{%- set yaml_metadata -%}
source_model: 
    'source_data': 'source_account'
ldts: 'PARSE_TIMESTAMP('%Y-%m-%dT%H-%M-%S', edwLoadDate)'
rsrc: "CONCAT(source_system, '||', source_object)"
hashed_columns: 
    hk_account_h:
        - account_number
        - account_key
    hd_account_s:
        is_hashdiff: true
        columns:
            - name
            - address
            - phone
            - email
    hk_account_contract_l:
        - account_number
        - account_name
        - contractnumber
derived_columns:
    conversion_duration:
        value: 'TIMESTAMP_DIFF(conversion_date, created_date, DAY)'
        datatype: 'INT64'
    country_isocode:
        value: '!GER'
        datatype: 'STRING'
    account_name:
        value: 'name'
        datatype: 'String'
prejoined_columns:
    contractnumber:
        src_name: 'source_data'
        src_table: 'contract'
        bk: 'contractnumber'
        this_column_name: 'ContractId'
        ref_column_name: 'Id'
multi_active_config:
    multi_active_key: 'ma_attribute'
    main_hashkey_column: 'hk_test'
enable_ghost_records: False
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

- **source_model**: The source model that you want to use for the stage is available as a source defined inside the .yml file with the name `source_data`, and you select the table `source_account` out of that source.
- **ldts**: Applies the SQL function `PARSE_TIMESTAMP` on the input column `edwLoadDate`.
- **rsrc**: Applies the SQL function `CONCAT` to concatenate two source columns.
- **hashed_columns**:
  - **hk_account_h**: A hub hashkey called `hk_account_h` is defined, that is calculated out of the two business keys `account_number` and `account_key`
  - **hd_account_s**: A hashdiff called `hd_account_s` is calculated out of the descriptive attributes `name`, `address`, `phone`, and `email`.
  - **hk_account_contract_l**: A link hashkey called `hk_account_contract_l` is defined, calculated of all business keys of the connected hubs.
- **derived_columns**:
  - **conversion_duration**: The column `conversion_duration` calculates the amount of days between two columns available inside the source data.
  - **country_isocode**: The column `country_isocode` inserts the static string `GER` for all rows.
  - **account_name**: The column `account_name` duplicates an already existing column and gives it another name.
- **prejoined_columns**:
  - **contractnumber**: Creates a column called `contractnumber` that holds values of the column with the same name (specified in `bk`) from the source table `contract` in the source `source_data` by joining on `this.ContractId = contract.Id`. In this case the prejoined column alias equals the name of the original business key column, which should be the case for most prejoins. But sometimes the same object is prejoined multiple times or a self-prejoin happens, and then you would have to rename the final columns to not have duplicate column names. That behaviour is seen in the next prejoined column.
- **multi_active_config**:
  - **multi_active_key**: The multi active key(s) inside the source data. The combination of all Business Keys and multi active keys needs to be unique per load date inside the source data.
  - **main_hashkey_column**: The one hashkey column, that would be unique over the stage when combined with the multi active key(s). Needs to be one of the columns that is generated inside hashed_columns and should be the hashkey of the Hub generated out of the source data.
- **enable_ghost_records**:
  - **False**: By passing False to this parameter no ghost records will be created in the resulting staging table.