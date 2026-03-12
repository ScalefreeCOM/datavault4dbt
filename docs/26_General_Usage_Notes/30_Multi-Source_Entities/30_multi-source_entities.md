---
sidebar_position: 30
sidebar_label: Multi-Source Entities
title: Multi-Source Entities
---

# MULTI-SOURCE ENTITIES

---

Datavault4dbt allows certain entity types to be configured to load from multiple source object. Following the Data Vault 2.0 best practices, the following entity types can be loaded from multiple sources:

- Hubs
- Standard Links
- Non-Historized Links
- Record Tracking Satellites
- Reference Hubs

## CONFIGURATION

To configure a dbt model of one of the entity types above, the `source_models` parameter needs to be set properly. Compared to a single-source entity, `source_models` should now be defined as a list.

__Please notice that `source_models` can also be defined as a dictionary, but due to better flexibility, we highly recommend defining it as a list.__

## HUB EXAMPLE

```jinja
source_models:
    - name: <source_1>
      bk_columns:
        - <bk_1>
        - <bk_2>
      rsrc_static: <rsrc_static_1>
    - name: <source_2>
      bk_columns:
        - <bk_1>
        - <bk_2>
      hk_column: <other_hk_name>
      rsrc_static: <rsrc_static_2>
hashkey: <target_hashkey_name>
business_keys: 
    - <target_bk_1>
    - <target_bk_2>
```

A couple of remarks:
- The source-specific configuration overwrites the higher-level configuration. Meaning that from `<source_1>` the column `<target_hashkey_name>` will be selected, while from `<source_2>` the column `<other_hk_name>` is selected.
- The high-level parameter definitions will influence the physical table structure regarding column names. The Hub will have the columns `<target_hashkey_name>`, `<target_bk_1>`, and `<target_bk_2>`. If not set, the definitions of the first source model influence the column names.
 
---

## SPECIAL CASES

### ONE SOURCE OBJECT REFERS TO ANOTHER ONE IN MULTIPLE ROWS

Imagine a source object **Contract** which refers to a **Client** object in three different roles, aka in three different columns. This would lead to a Link table, that connects one Contract to 1-3 different Clients, via three different Foreign-Keys.

The first step here is to generate all required hashkeys in a staging model:

### STAGE CONTRACTS

```jinja
hashed_columns:
    hk_Contract_h:
        - Contract_ID
    hk_Client_Buyer_h:
        - Buyer_Client_ID
    hk_Client_Seller_h:
        - Seller_Client_ID
    hk_Client_Broker_h:
        - Broker_Client_ID
    hk_Contract_l:
        - Contract_ID
        - Buyer_Client_ID
        - Seller_Client_ID
        - Broker_Client_ID
```

Additionally there might be a simple Client source object, with a stage like this:

### STAGE CLIENTS

```jinja
hashed_columns:
    hk_Client_h:
        - Client_ID
```

To assure that all Clients referenced in the Contracts object are present in the Hub Clients, we want to load the Hub Clients not only from the Client source object, but also from the Contracts object. Therefore we define the Clients Hub models metadata as the following:

### HUB CLIENTS

```jinja
{%- set yaml_metadata -%}
hashkey: 'hk_Client_h'
business_keys: 
    - Client_ID
source_models: 
    - name: stage_clients
      rsrc_static: '!Clients'
    - name: stage_contracts
      hk_column: hk_Client_Buyer_h
      bk_columns: Buyer_Client_ID
      rsrc_static: '!Contracts_Buyers'
    - name: stage_contracts
      hk_column: hk_Client_Seller_h
      bk_columns: Seller_Client_ID
      rsrc_static: '!Contracts_Sellers'
    - name: stage_contracts
      hk_column: hk_Client_Broker_h
      bk_columns: Broker_Client_ID
      rsrc_static: '!Contracts_Brokers'
{%- endset -%}
```

Remarks:
- For the stage_clients no config for hashkeys and business keys is done, because the high-level configuration works here.
- The stage_contracts is referenced three times, each selecting another pair of hashkey/business key columns.