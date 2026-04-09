---
sidebar_position: 33
sidebar_label: Redshift
title: Redshift
---

# REDSHIFT

---

## How and when to use the Distkey in Redshift

The effective utilization of Distkey in AWS Redshift significantly impacts query performance by optimizing table organization. Tables containing fewer than 10,000 records benefit from the `DISTSTYLE ALL` command for distribution. For tables exceeding 10,000 records, employing the `DISTSTYLE KEY` command enhances organization. When designating the Distkey for a Hub and Satellite, prioritize the Hash Key. For Link Distkeys, select the Hash Key of the referenced Hub with the highest cardinality for optimal performance (dbt documentation on how to implement)

## Avoid unnecessary satellite splits

To counter AWS Redshift’s poor join performance, it’s advisable to minimize unnecessary satellite splits. By doing so, the number of required joins decreases, directly enhancing the overall performance of the data vault.