---
sidebar_position: 21
sidebar_label: Reference Table
title: Reference Table
---

# REFERENCE TABLE

---

Data-history related, this can be done in three different ways, all supported by datavault4dbt:

- **latest data**: Only the latest descriptive state per reference key(s) is loaded into a reference table. Minimum disk space (or computing power when using views) required.
- **fully historized**: The entire history of the descriptive data per reference key(s) is loaded into the reference table. Depending on the change-frequency in the data, this can consume a lot of disk space (or computing power when using views).
- **snapshot driven**: Similar to a PIT, a pre-defined set of snapshot dates is used to get the valid state of descriptive data for each snapshot. To use this historization method, a **snapshot v1 view** is required.

### REQUIRED PARAMETERS

| Parameters     | Data Type            | Required  | Default Value | Explanation |
|----------------|----------------------|-----------|---------------|-------------|
| ref_hub        | string               | mandatory | –             | Name of the underlying reference Hub. |
| ref_satellites | list \| dictionary   | mandatory | –             | Name(s) of all reference Satellites that are connected to the reference Hub and should be used for this reference Table. Either define as a list of satellites, or define as a dictionary. When defining as a dictionary, the parameters `include` or `exclude` can be used to select only a subset of columns for each satellite. When using `include` only the specified columns will be selected from that satellite. When using `exclude` all columns except the ones specified will be selected from the satellite. Both parameters cannot be combined for a single satellite. |

### OPTIONAL PARAMETERS

| Parameters                                     | Data Type                         | Required | Default Value                          | Explanation |
|------------------------------------------------|-----------------------------------|----------|----------------------------------------|-------------|
| historized                                     | ['full', 'latest', 'snapshot']    | optional | 'latest'                               | Controls how the data in the reference table should be historized. The three allowed values are `full`, `latest` and `snapshot`. For details what each means see above. When selecting `snapshot`, the additional parameter `snapshot_relation` (see next table line) must be defined. |
| snapshot_relation (when choosing 'snapshot')  | string                            | optional | None                                   | Name of the dbt model for the snapshot v1 view. Must already be available in the dbt project. |
| snapshot_trigger_column (when choosing 'snapshot') | string                       | optional | datavault4dbt.snapshot_trigger_column  | Name of the trigger column inside the `snapshot_relation`. |
| src_ldts                                       | string                            | optional | datavault4dbt.ldts_alias               | Name of the ldts column inside the source models. Needs to use the same column name as defined as alias inside the staging model. |
| src_rsrc                                       | string                            | optional | datavault4dbt.rsrc_alias               | Name of the rsrc column inside the source models. Needs to use the same column name as defined as alias inside the staging model. |

## EXAMPLE 1 (LATEST DATA)

```jinja
{{ config(schema='core', materialized='view') }}

{%- set yaml_metadata -%}
ref_hub: 'nation_rh'
ref_satellites: 
    nation_rs1:
        include:
            - N_NAME
historized: 'latest'
{%- endset -%}

{{ datavault4dbt.ref_table(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

With this example, a reference table for the reference Hub `nation_rh` is created. It will only hold the latest set of descriptive data per reference key(s).

- **ref_hub**:
  - __nation_rh__: This reference table will use the reference Hub for Nation as a base,
- **ref_satellites**:
  - __nation_rs__: Only one satellite is used for this reference table. Since the parameter `include` is specified, only the descriptive attribute `N_NAME` will be loaded into the reference Table
- **historized**:
  - __latest__: The reference table is configured to only hold the latest descriptive data per reference key(s). Therefore it is not required to define `snapshot_relation` and `snapshot_trigger_column`.

### COMPILED SQL

<details>
  <summary>Click me</summary>
```sql
WITH 

dates AS (

SELECT MAX(ldts) as ldts FROM (SELECT distinct 
        ldts
    FROM datavault4dbt_demo.core_Core.nation_rs1
    WHERE ldts != TO_TIMESTAMP('8888-12-31T23:59:59', 'YYYY-MM-DDTHH24:MI:SS')
    
    )


),

ref_table AS (

    SELECT
    
        h.N_NATIONKEY,
        ld.ldts,
        h.rsrc,
        s_1.N_NAME 

    FROM datavault4dbt_demo.core_Core.nation_rh h
    
    FULL OUTER JOIN dates ld
        ON 1 = 1  

    LEFT JOIN datavault4dbt_demo.core_Core.nation_rs1 s_1
        ON h.N_NATIONKEY = s_1.N_NATIONKEY
        AND  ld.ldts BETWEEN s_1.ldts AND s_1.ledts
    
    

    WHERE h.ldts <= ld.ldts

) 

SELECT * FROM ref_table
```
</details>

## EXAMPLE 2 (FULLY HISTORIZED)

```jinja
{{ config(schema='core', materialized='view') }}

{%- set yaml_metadata -%}
ref_hub: 'nation_rh'
ref_satellites: 
    nation_rs1:
        exclude:
            - N_NAME
historized: 'full'
{%- endset -%}

{{ datavault4dbt.ref_table(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

With this example, a reference table for the reference Hub `nation_rh` is created. It will hold all states of descriptive data.

- **ref_hub**:
  - __nation_rh__: This reference table will use the reference Hub for Nation as a base,
- **ref_satellites**:
  - __nation_rs__: Only one satellite is used for this reference table. Since the parameter `exclude` is defined, only the descriptive columns of the satellite, that do not match the name `N_NAME` will be loaded into the reference table.
- **historized**:
  - __full__: The reference table is configured hold all the states of descriptive data. Therefore it is not required to define `snapshot_relation` and `snapshot_trigger_column`.

### COMPILED SQL

<details>
  <summary>Click me</summary>
```sql
WITH 

dates AS (

SELECT distinct ldts FROM (SELECT distinct 
        ldts
    FROM datavault4dbt_demo.core_Core.nation_rs1
    WHERE ldts != TO_TIMESTAMP('8888-12-31T23:59:59', 'YYYY-MM-DDTHH24:MI:SS')
    
    )


),

ref_table AS (

    SELECT
    
        h.N_NATIONKEY,
        ld.ldts,
        h.rsrc,
        s_1.HD_NATION_RS,
        s_1.N_COMMENT,
        s_1.N_REGIONKEY 

    FROM datavault4dbt_demo.core_Core.nation_rh h
    
    FULL OUTER JOIN dates ld
        ON 1 = 1  

    LEFT JOIN datavault4dbt_demo.core_Core.nation_rs1 s_1
        ON h.N_NATIONKEY = s_1.N_NATIONKEY
        AND  ld.ldts BETWEEN s_1.ldts AND s_1.ledts
    
    

    WHERE h.ldts <= ld.ldts

) 

SELECT * FROM ref_table   
```
</details>

## EXAMPLE 3 (SNAPSHOT BASED)

```jinja
{{ config(schema='core', materialized='incremental') }}

{%- set yaml_metadata -%}
ref_hub: 'nation_rh'
ref_satellites: 
    - nation_rs1
historized: 'snapshot'
snapshot_relation: 'snap_v1'
{%- endset -%}

{{ datavault4dbt.ref_table(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

With this example, a snapshot-based reference table for the reference hub “nation_rh” is created.

- **ref_hub**:
  - __nation_rh__: This reference table will use the reference Hub for Nation as a base,
- **ref_satellites**:
  - __nation_rs__: Only one satellite is used for this reference table. Since the parameters “include” or “exclude” are not defined for this satellite, all descriptive columns of this satellites will end up in the reference table.
- **historized**:
  - __snapshot__: The reference table is configured to be snapshot based historized. That requires a snapshot relation, which is set next.
- **snapshot_relation**:
  - __snap_v1__: The dbt model that holds the Snapshot v1 View is called `snap_v1`. This must be set when historization is set to `snapshot. The snapshot model must already exists within the dbt project.
- **snapshot_trigger_column**:
  - __not set__: Since this parameter is not set, the global variable “datavault4dbt.snapshot_trigger_column” will be used for activating and deactivating specific snapshots within the snapshot relation.

### COMPILED SQL

<details>
  <summary>Click me</summary>
```sql
WITH 

dates AS (

SELECT 
        sdts
    FROM (
        
        SELECT 
            sdts
        FROM datavault4dbt_demo.core_Control.snap_v1
        WHERE is_active
    )),

ref_table AS (

    SELECT
    
        h.N_NATIONKEY,
        ld.sdts,
        h.rsrc,
        s_1.HD_NATION_RS,
        s_1.N_COMMENT,
        s_1.N_NAME,
        s_1.N_REGIONKEY 

    FROM datavault4dbt_demo.core_Core.nation_rh h
    
    FULL OUTER JOIN dates ld
        ON 1 = 1  

    LEFT JOIN datavault4dbt_demo.core_Core.nation_rs1 s_1
        ON h.N_NATIONKEY = s_1.N_NATIONKEY
        AND  ld.sdts BETWEEN s_1.ldts AND s_1.ledts
    
    

    WHERE h.ldts <= ld.sdts

) 

SELECT * FROM ref_table
```
</details>
