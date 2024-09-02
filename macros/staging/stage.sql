  {#
    This macro creates the staging layer for the Data Vault model. This layer is mainly for hashing, and additionally gives the option to create derived columns, conduct prejoins and add NULL values for
    missing columns. Always create one stage per source table that you want to add to the Data Vault model. The staging layer is not to harmonize data. That will be done in the later layers.

    Parameters:

    ldts::string                        Name of the column inside the source data, that holds information about the Load Date Timestamp. Can also be a SQL expression.

                                        Examples:
                                            'edwLoadDate'                                           Uses the column called 'edwLoadDate' as it is from the source model.
                                            'PARSE_TIMESTAMP('%Y-%m-%dT%H-%M-%S', edwLoadDate)'     Applies the SQL function 'PARSE_TIMESTAMP' on the input column 'edwLoadDate'.

    rsrc::string                        Name of the column inside the source data, that holds information about the Record Source. Can also be a SQL expression
                                        or a static string. A static string must begin with a '!'.

                                        Examples:
                                            'edwRecordSource'                               Uses the column called 'edwRecordSource' as it is from the source model.
                                            '!SAP.Accounts'                                 Uses the static string 'SAP.Customers' as rsrc.
                                            'CONCAT(source_system, '||', source_object)'    Applies the SQL function 'CONCAT' to concatenate two source columns.

    source_model::string | dictionary   Can be just a string holding the name of the referred dbt model to use as a source. But if the 'source' functionality inside
                                        the .yml file is used, it must be a dictionary with 'source_name': 'source_table'.

                                        Examples:
                                            'source_account'                        The source model that you want to use for the stage is available as another dbt model with the name 'source_account'.
                                            {'source_data': 'source_account'}       The source model that you want to use for the stage is available as a source defined inside the .yml file
                                                                                    with the name 'source_data', and you select the table 'source_account' out of that source.

    include_source_columns::boolean     Defines if all columns from the referred source table should be included in the result table, or if only the added columns should
                                        be part of the result table. By default the source columns should be included.

    hashed_columns::dictionary          Defines the names and input for all hashkeys and hashdiffs to create. The key of each hash column is the name of the hash column.
                                        The value for Hashkeys is a list of input Business Keys, for Hashdiffs another dictionary with the pairs 'is_hashdiff:true' and
                                        'columns: <list of columns>'.

                                        Examples:
                                            {'hk_account_h': ['account_number', 'account_key'],                         A hashkey called 'hk_account_h' is defined, that is calculated out of the two business
                                             'hd_account_s': {'is_hashdiff': true,                                      keys 'account_number' and 'account_key'. A hashdiff called 'hd_account_s' is calculated
                                                              'columns': ['name', 'address', 'phone', 'email']}}        out of the descriptive attributes 'name', 'address', 'phone', and 'email'. More hashkeys
                                                                                                                        and hashdiffs would be added as other keys of the dictionary.

    derived_columns::dictionary         Defines values and datatypes for derived ('added' or 'calculated') columns. The values of this dictionary are the desired column names,
                                        the value is another dictionary with the keys 'value' (holding a column name, a SQL expression, or a static string beginning with '!') and
                                        'datatype' (holding a valid SQL datatype for the target database).

                                        Examples:
                                            {'conversion_duration': {'value': 'TIMESTAMP_DIFF(conversion_date, created_date, DAY)',     Creates three derived columns. The column 'conversion_duration' calculates
                                                                     'datatype': 'INT64'},                                              the number of days between two columns available inside the source data.
                                             'country_isocode':     {'value': '!GER',                                                   The column 'country_isocode' inserts the static string 'EUR' for all rows.
                                                                     'datatype': 'STRING'},                                             The column 'account_name' duplicates an already existing column and gives
                                             'account_name':        {'value': 'name',                                                   it another name. More derived columns can be added as other keys of
                                                                     'datatype': 'String'}}                                             the dictionary.

    sequence::string                    Name of the column inside the source data, that holds a sequence number that was generated during the data source extraction process.
                                        Optional and not required.

                                        Example:
                                            'edwSequence'       Uses the column 'edwSequence' that is available inside the source data.

    prejoined_columns::dictionary       Defines information about information that needs to be prejoined. Most commonly used to create links, when the source data does not
                                        hold the Business Key, but the technical key of the referred object. The values of the dict are the aliases you want to give the prejoined
                                        columns. Typically, but not always, this should be the same as the name of the prejoined column inside the prejoined entity. For each prejoined column
                                        a few things need to be defined inside another dictionary now. 'src_name' holds the name of the source of the prejoined entity, as defined
                                        in the .yml file. 'src_table' holds the name of the prejoined table, as defined inside the .yml file. 'bk' holds the name of the business key column
                                        inside the prejoined table. 'this_column_name' holds the name of the column inside the original source data, that refers to the prejoined table.
                                        'ref_column_name' holds the name of the column, that is referred by 'this_column_name' inside the prejoined table.

                                        Example:
                                            {'contractnumber':  {'src_name': 'source_data',                 Prejoins with two other entities to extract one Business Key each. Creates a
                                                                'src_table': 'contract',                    column called 'contractnumber' that holds values of the column with the same
                                                                'bk': 'contractnumber',                     name (specified in 'bk') from the source table 'contract' in the source 'source_data'
                                                                'this_column_name': 'ContractId',           by joining on 'this.ContractId = contract.Id'. In this case the prejoined
                                                                'ref_column_name': 'Id'},                   column alias equals the name of the original business key column, which should be
                                            'master_account_key' {'ref_model': 'account_prep',              or a self-prejoin happens, and then you would have to rename the final columns to not
                                                                'bk': 'account_key',                        have duplicate column names. The column 'master_account_key' holds values of the column
                                                                'this_column_name': 'master_account_id',    'account_key' inside the pre-populated dbt model 'account_prep'. If this prejoin is done inside account,
                                                                'ref_column_name': 'Id'}}                   we would now have a self-prejoin ON 'account.master_account_id = account.Id'. Because
                                                                                                            the table 'account' already has a column 'account_key', we rename the prejoined column
                                                                                                            to 'master_account_key'. More prejoined columns can be added as other keys of the dictionary.

    missing_columns::dictionary         If the schema of the source changes over time and columns are disappearing, this parameter gives you the option to create additional columns
                                        holding NULL values, that replace columns that were previously there. By this procedure, hashdiff calculations and satellite payloads wont break.
                                        The dictionary holds the names of those columns as keys, and the SQL datatypes of these columns as values.

                                        Example:
                                            {'legacy_account_uuid': 'INT64',        Two additional columns are added to the source table holding NULL values. The column 'legacy_account_uuid' will
                                             'shipping_address'   : 'STRING'}       have the datatype 'INT64' and the column 'shipping_address' will have the datatype 'STRING'.

    multi_active_config::dictionary     If the source data holds multi-active data, define here the column(s) holding the multi-active key and the main hashkey column. If the source data is multi-active but has no natural multi-active
                                        key, create one using the row_number SQL function (or similar) one layer before. Then insert the name of that artificial column into the multi-active-key parameter.
                                        The combination of the multi-active key(s), the main-hashkey and the ldts column should be unique in the final result satellite. 
                                        If not set, the stage will be treated as a single-active stage. 

                                        Example: 
                                            {'multi_active_key': 'phonetype',               This source data has a column called 'phonetype' that holds the multi-active key. 'hk_contact_h' is defined as the main hashkey. 
                                             'main_hashkey_column': 'hk_contact_h'}         That means, that the combination of main_hashkey, ldts and 'phonetype' is unique inside the source system.    

                                            {'multi_active_key': ['phonetype', 'company'],  This source data comes with two multi-active keys. The combination of those two, the main_hashkey and ldts is unique 
                                             'main_hashkey_column': 'hk_contact_h'}         inside the source system.          

  #}



  {%- macro stage(ldts, rsrc, source_model, include_source_columns=true, hashed_columns=none, derived_columns=none, sequence=none, prejoined_columns=none, missing_columns=none, multi_active_config=none) -%}
    
    {# If include_source_columns is passed but its empty then it is set with the default value (true) #}
    {%- if include_source_columns is none or include_source_columns == "" -%}
      {%- set include_source_columns = true -%}
    {%- endif -%}

    {{- adapter.dispatch('stage', 'datavault4dbt')(include_source_columns=include_source_columns,
                                        ldts=ldts,
                                        rsrc=rsrc,
                                        source_model=source_model,
                                        hashed_columns=hashed_columns,
                                        derived_columns=derived_columns,
                                        sequence=sequence,
                                        prejoined_columns=prejoined_columns,
                                        missing_columns=missing_columns,
                                        multi_active_config=multi_active_config) -}}

{%- endmacro -%}
