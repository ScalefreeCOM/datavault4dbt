---
sidebar_position: 27
sidebar_label: Defining Macro Parameter
title: Defining Macro Parameter
---

# DEFINING MACRO PARAMETER

---

All datavault4dbt front-end macros have many parameters to configure the generated SQL code. These parameters can be defined in various ways, this page focuses on the most efficient ways.

---

### DIRECTLY SETTING YAML_METADATA (NEW & RECOMMENDED!)

Since datavault4dbt version 1.9, parameters can be directly inputted as the yaml_metadata, allowing multiple parameters to be set in one go.

An example Hub which uses this style of parameter setting looks like this:

```jinja
{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
hashkey: 'hk_account_h'
business_keys: 
    - account_key
    - account_number
source_models: stage_account
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}
```

### SETTING INDIVIDUAL PARAMETER

The classic way of setting parameters in datavault4dbt already included yaml_metadata, but it was turned into individual parameters first, before passing them to the macro. The example from above would look like this with individually set parameters:

```jinja
{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
hashkey: 'hk_account_h'
business_keys: 
    - account_key
    - account_number
source_models: stage_account
{%- endset -%}

{%- set metadata_dict = fromyaml(yaml_metadata) -%}

{{ datavault4dbt.hub(hashkey=metadata_dict.get('hashkey'),
                    business_keys=metadata_dict.get('business_keys'),
                    source_models=metadata_dict.get('source_models')
                    ) }}
```

Notice that this model is already a bit longer than the one which directly passes yaml_metadata!

### COMBINING BOTH APPROACHES (NOT RECOMMENDED)

All front-end macros are designed to work with both approaches, and to allow a combination of both ways of passing parameters.
**We strongly recommend picking one style of passing parameters. Best, choose one for all your models, but at least don’t mix both styles in one model. You might encounter unexpected behavior.**

A (bad) example of combining both styles would look like this:

```jinja
{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
hashkey: 'hk_account_h'
business_keys: 
    - account_key
    - account_number
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata,
                    source_models=stage_account
                    ) }}
```

This example would not cause any unexpected behavior.

If a parameter is passed in both ways, individually and in the yaml_metadata, like in this example:

```jinja
{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
hashkey: 'hk_account_h'
business_keys: 
    - account_key
    - account_number
source_models: stage_account
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata,
                    source_models=stage_customer
                    ) }}
```

**The individual parameter is ignored! This hub will be loaded from stage_account, as defined in the yaml_metadata.**