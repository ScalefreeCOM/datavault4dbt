{% docs stage %}

## Stage

Creates the staging layer for the Data Vault model. This layer is mainly for hashing, and additionally gives the
option to create derived columns, conduct prejoins and add NULL values for missing columns. Always create one stage
per source table that you want to add to the Data Vault model. The staging layer is not to harmonize data — that
will be done in the later layers.

### Usage

```jinja
{% raw %}
{{ datavault4dbt.stage(
    source_model='source_account',
    ldts='edwLoadDate',
    rsrc='!SAP.Accounts',
    hashed_columns={
        'hk_account_h': ['account_number', 'account_key'],
        'hd_account_s': {
            'is_hashdiff': true,
            'columns': ['name', 'address', 'country', 'phone', 'email']
        }
    },
    derived_columns={
        'country_isocode': {'value': '!GER', 'datatype': 'STRING'}
    }
) }}
{% endraw %}
```

### Multi-active stage

```jinja
{% raw %}
{{ datavault4dbt.stage(
    source_model='source_contact_phones',
    ldts='edwLoadDate',
    rsrc='!CRM.ContactPhones',
    hashed_columns={
        'hk_contact_h': ['contact_id'],
        'hd_contact_phonenumber_s': {
            'is_hashdiff': true,
            'columns': ['phone_type', 'phone_number']
        }
    },
    multi_active_config={
        'multi_active_key': 'phone_type',
        'main_hashkey_column': 'hk_contact_h'
    }
) }}
{% endraw %}
```

### Metadata block usage

`meta` is a Jinja string variable containing all macro parameters as YAML. Define it using a
`set` block in the model SQL file — the macro parses the YAML string at runtime:

```jinja
{% raw %}
{%- set meta -%}
source_model: 'source_account'
ldts: 'edwLoadDate'
rsrc: '!SAP.Accounts'
hashed_columns:
  hk_account_h:
    - account_number
    - account_key
  hd_account_s:
    is_hashdiff: true
    columns:
      - name
      - address
      - country
      - phone
      - email
derived_columns:
  country_isocode:
    value: '!GER'
    datatype: STRING
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=meta) }}
{% endraw %}
```

{% enddocs %}
