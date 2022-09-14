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

{%- set metadata_dict = fromyaml(yaml_metadata) -%}

{%- set source_model = metadata_dict['source_model'] -%}
{%- set ldts = metadata_dict['ldts'] -%}
{%- set rsrc = metadata_dict['rsrc'] -%}
{%- set hashed_columns = metadata_dict['hashed_columns'] -%}
{%- set derived_columns = metadata_dict['derived_columns'] -%}
{%- set prejoined_columns = metadata_dict['prejoined_columns'] -%}
{%- set missing_columns = metadata_dict['missing_columns'] -%}

{{ datavault4dbt.stage(source_model=source_model,
                       ldts=ldts,
                       rsrc=rsrc,
                       hashed_columns=hashed_columns,
                       derived_columns=derived_columns,
                       prejoined_columns=prejoined_columns,
                       missing_columns=missing_columns) }}
----------------------------------------------------------------------------------------------------------------
Description:

source_model:
The source model that you want to use for the stage is available as another dbt model with the name 'source_account'.

ldts:
Uses the column called 'edwLoadDate' as it is from the source model.

rsrc:
Uses the column called 'edwRecordSource' as it is from the source model.

hashed_columns:

    hk_account_h:
    A hashkey called 'hk_account_h' is defined, that is calculated out of the two business
    keys 'account_number' and 'account_key'

    hd_account_s:
    A hashdiff called 'hd_account_s' is calculated out of the descriptive attributes 
    'name', 'address', 'phone', and 'email'.

derived_columns:

    conversion_duration:
    The column 'conversion_duration' calculates the amount of days between two columns available inside the source data.

    country_isocode:
    The column 'country_isocode' inserts the static string 'EUR' for all rows.

    account_name:
    The column 'account_name' duplicates an already existing column and gives it another name.

prejoined_columns:

    contractnumber:
    Creates a column called 'contractnumber' that holds values of the column with the same name (specified in 'bk')
    from the source table 'contract' in the source 'source_data' by joining on 'this.ContractId = contract.Id'. 
    In this case the prejoined column alias equals the name of the original business key column, which should be
    the case for most prejoins. But sometimes the same object is prejoined multiple times or a self-prejoin happens, 
    and then you would have to rename the final columns to not have duplicate column names. That behaviour is seen
    in the next prejoined column.

    master_account_key:
    The column 'master_account_key' holds values of the column 'account_key' inside the source table 'account'. 
    If this prejoin is done inside account, we would now have a self-prejoin ON 'account.master_account_id = account.Id'. 
    Because the table 'account' already has a column 'account_key', we rename the prejoined column  to 'master_account_key'.

missing_columns:
Two additional columns are added to the source table holding NULL values. The column 'legacy_account_uuid' will
have the datatype 'INT64' and the column 'shipping_address' will have the datatype 'STRING'.