### How to create a Stage model for your source data

## Requirements
To create a stage model, you need to ensure that the source data is available within the database, that you chose for storing your raw- & business-vault.
Ideally all your source data would be stored in a seperate schema / dataset with the name <source_schema>.

Additionally your source data needs to be flat & wide. If your currently available data still holds nested columns, you have to flatten it before you can start working with this version of datavault4dbt.

## How to start
1. At first you need to create (or extend) a .yml file inside your models folder. We recommend to have one folder for each EDW layer, therefor we create a file "models/stage/test_source_system.yml".
  1.1 This file should look like this:

