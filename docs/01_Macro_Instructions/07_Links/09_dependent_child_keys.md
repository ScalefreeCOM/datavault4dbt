---
sidebar_position: 9
sidebar_label: Dependent Child Keys
title: Dependent Child Keys
---

# DEPENDENT CHILD KEYS

---

Some Links require a dependent child key to create a unique combination of columns, such as a line-item number. Let`s think of an example object representing invoices. An Invoice is connecting multiple products. Therefore the combination of an invoice number and a product number is not unique. The uniqueness is only achieved, when the line-item number is added.

To model a dependent child key with datavault4dbt, you have to consider both the staging model and the link model. The dependent child key would be added to the input of the link hashkey calculation, to achieve uniqueness inside the link. Additionally, the unhashed dependent child key is added as a column to the link model.

## STAGING MODEL

To extend the link hashkey with the dependent child key, the staging model for invoices must have a section for “hashed_columns” where the link hashkey is defined properly. Let`s have a look at an example staging model:

### STG_INVOICES.SQL

```jinja
{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: 'source_invoices'
ldts: 'edwLoadDate'
rsrc: 'edwRecordSource'
hashed_columns: 
    hk_invoices_products_l:
        - invoice_number
        - product_no
        - line_item_no
    hk_product_h:
        - product_no 
    hk_invoices_h:
        - invoice_number 
    hd_invoices_s:
        is_hashdiff: true
        columns: 
            - invoice_date
            - total_sum
            - payment_method
            - shipping_method
    hd_invoices_products_s:
        is_hashdiff: true
        columns:
            - quantity
            - discount 
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}
```

In this example, the hashkey for an invoices_products_l would be calculated out of the two business keys `invoice_number` and `product_no` plus the dependent child key `line_item_no`.

## LINK MODEL

### INVOICES_PRODUCTS_L.SQL

```jinja
{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
link_hashkey: 'hk_invoices_products_l'
foreign_hashkeys: 
    - 'hk_product_h'
    - 'hk_invoices_h'
    - 'line_item_no'
source_models: stg_invoices
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}
```

On top of the previously created stage model, a link between invoices and products is created, as shown in the example model above. Compared to a regular link without a dependent child key, we have hijacked the `foreign_hashkeys` parameter to additionally include the dependent child key in the link table structure.

## CONCLUSION

Modeling a dependent child key with datavault4dbt is very straight forward. Users just need to add the dependent child key to the link hashkey calculation inside the staging model. Afterwards they add the dependent child key to the foreign hashkeys inside the link model.