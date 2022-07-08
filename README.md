Welcome to the Scalefree version of dbtVault!

### Included Macros
- Staging Area (For Hashing, prejoins and ghost records)
- Hubs, Links & Satellites (allowing multiple deltas)
- Virtualized End-Dating (in Satellites)
- PIT Tables
- Source Marts
- Data Vault 2.0 related tests

### Requirements

To use the macros efficiently, there are a few prerequisites you need to provide:
- Flat & Wide source data, available within your target database
- Load Date column that represents the arriving time in the source data storage
- Record Source column that gives information about where the source data is coming from (e.g. the file location inside a Data Lake)

### Features

With the Scalefree version of dbtvault you will get a lot of awesome features, including:
- A Data Vault 2.0 implementation congruent to the original Data Vault 2.0 definition by Dan Linstedt and Michael Olschimke
- A fully auditable solution for a Data Warehouse
- A modern insert-only approach that avoids updating data
- A straight-forward, standardized approach to conduct agile datawarehouse development cycles

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
