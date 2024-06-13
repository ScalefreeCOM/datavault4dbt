# datavault4dbt by [Scalefree International GmbH](https://www.scalefree.com)

<img src="https://user-images.githubusercontent.com/81677440/195860893-435b5faa-71f1-4e01-969d-3593a808daa8.png">


---

### Included Macros
- Staging Area (For Hashing, prejoins and ghost records)
- Hubs, Links & Satellites (allowing multiple deltas)
- Non-Historized Links and Satellites
- Multi-Active Satellites
- Virtualized End-Dating (in Satellites)
- Reference Hubs, - Satellites, and - Tables
- PIT Tables
  - Hook for Cleaning up PITs 
- Snapshot Control

### Features
With datavault4dbt you will get a lot of awesome features, including:
- A Data Vault 2.0 implementation congruent to the original Data Vault 2.0 definition by Dan Linstedt
- Ready for both Persistent Staging Areas and Transient Staging Areas, due to the allowance of multiple deltas in all macros, without losing any intermediate changes - Enforcing standards in naming conventions by implementing [global variables](https://github.com/ScalefreeCOM/datavault4dbt/wiki/Global-variables) for technical columns    
- A fully auditable solution for a Data Warehouse
- Creating a centralized, snapshot-based Business interface by using a centralized snapshot table supporting logarithmic logic
- A modern insert-only approach that avoids updating data
- Optimizing incremental loads by implementing a high-water-mark that also works for entities that are loaded from multiple sources
- A straight-forward, standardized approach to conduct agile datawarehouse development cycles

### Requirements

To use the macros efficiently, there are a few prerequisites you need to provide:
- Flat & Wide source data, available within your target database
- Load Date column that represents the arriving time in the source data storage
- Record Source column that gives information about where the source data is coming from (e.g. the file location inside a Data Lake)

<img src="https://www.getdbt.com/ui/img/logos/dbt-logo.svg" width=33% align=right>

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
- Find [dbt events](https://events.getdbt.com) near you
- Check out the [Scalefree-Blog](https://www.scalefree.com/blog/)
   - [Data-Vault 2.0 with dbt #1](https://www.scalefree.com/scalefree-newsletter/data-vault-2-0-with-dbt-part-1/)
   - [Data-Vault 2.0 with dbt #2](https://www.scalefree.com/scalefree-newsletter/data-vault-2-0-with-dbt-part-2/)
   - [Using Multi-Active-Satellites #1](https://www.scalefree.com/scalefree-newsletter/using-multi-active-satellites-the-correct-way-1-2/)
   - [Using Multi-Active-Satellites #2](https://www.scalefree.com/scalefree-newsletter/using-multi-active-satellites-the-correct-way-2-2/)
   - [Non-Historized Links](https://www.scalefree.com/modeling/the-value-of-non-historized-links/)
   - [Bridge Tables](https://www.scalefree.com/scalefree-newsletter/bridge-tables-101/)
   - [PIT Tables](https://www.scalefree.com/scalefree-newsletter/point-in-time-tables-insurance/)
   - [Hash Keys in Data-Vault](https://www.scalefree.com/architecture/hash-keys-in-the-data-vault/)


## Supported platforms:
Currently supported platforms are:
* Google Bigquery
* Exasol
* Snowflake
* PostgreSQL
* Amazon Redshift
* Microsoft Azure Synapse

We are working continuously at high pressure to adapt the package for large variety of different platforms. In the future, the package will hopefully be available for SQL Server, Oracle and many more.

---


## Installation instructions

1. Include this package in your `packages.yml` -- check [here](https://hub.getdbt.com/scalefreecom/datavault4dbt/latest/)
for installation instructions.
2. Run `dbt deps`

For further information on how to install packages in dbt, please visit the following link: 
[https://docs.getdbt.com/docs/building-a-dbt-project/package-management](https://docs.getdbt.com/docs/building-a-dbt-project/package-management#how-do-i-add-a-package-to-my-project)

### Global variables
datavault4dbt is highly customizable by using many global variables. Since they are applied on multiple levels, a high rate of standardization across your data vault 2.0 solution is guaranteed. The default values of those variables are set inside the packages `dbt_project.yml` and should be copied to your own `dbt_project.yml`. For an explanation of all global variables see [the wiki](https://github.com/ScalefreeCOM/datavault4dbt/wiki/Global-variables).

---
## Usage
The datavault4dbt package provides macros for Staging and Creation of all DataVault-Entities you need, to build your own DataVault2.0 solution. The usage of the macros is well-explained in the documentation: https://github.com/ScalefreeCOM/datavault4dbt/wiki

---
## Contributing
[View our contribution guidelines](CONTRIBUTING.md)

---
## License
[Apache 2.0](LICENSE.md)

[<img src="https://user-images.githubusercontent.com/81677440/196627704-e230a88f-270a-44b2-a07d-dcd06694bd48.jpg" width = 65% align=right>](https://www.scalefree.com)

## Contact
For questions, feedback, etc. reach out to us via datavault4dbt@scalefree.com!

