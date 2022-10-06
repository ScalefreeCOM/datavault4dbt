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
{%- endset -%}

{%- set metadata_dict = fromyaml(yaml_metadata) -%}

{%- set source_model = metadata_dict['source_model'] -%}
{%- set ldts = metadata_dict['ldts'] -%}
{%- set rsrc = metadata_dict['rsrc'] -%}
{%- set hashed_columns = metadata_dict['hashed_columns'] -%}
{%- set derived_columns = metadata_dict['derived_columns'] -%}
{%- set prejoined_columns = metadata_dict['prejoined_columns'] -%}

{{ datavault4dbt.stage(source_model=source_model,
                       ldts=ldts,
                       rsrc=rsrc,
                       hashed_columns=hashed_columns,
                       derived_columns=derived_columns,
                       prejoined_columns=prejoined_columns,
                       missing_columns=none) }}
----------------------------------------------------------------------------------------------------------------
Description:

source_model:
The source model that you want to use for the stage is available as a source defined inside the .yml file
with the name 'source_data', and you select the table 'source_account' out of that source.

ldts:
Applies the SQL function 'PARSE_TIMESTAMP' on the input column 'edwLoadDate'.

rsrc:
Applies the SQL function 'CONCAT' to concatenate two source columns.

hashed_columns:

    hk_account_h:
    A hub hashkey called 'hk_account_h' is defined, that is calculated out of the two business
    keys 'account_number' and 'account_key'

    hd_account_s:
    A hashdiff called 'hd_account_s' is calculated out of the descriptive attributes 
    'name', 'address', 'phone', and 'email'.

    hk_account_contract_l:
    A link hashkey called 'hk_account_contract_l' is defined, calculated of all business keys of the
    connected hubs. 

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
  