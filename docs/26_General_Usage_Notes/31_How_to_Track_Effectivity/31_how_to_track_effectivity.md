---
sidebar_position: 31
sidebar_label: How to Track Effectivity
title: How to Track Effectivity
---

# HOW TO TRACK EFFECTIVITY

---

When it comes to tracking effectivity with Data Vault 2.0, there are a few different ways on how to do that.

datavault4dbt covers three ways on how to track effectivity of business keys and relationships. The following image shows the basic decision tree:

![Effectivity Decision Tree](./img/Effectivity-Satellites-DBT-Public-Package-Decision-Tree-2048x539.avif)

The basic decision influencer is the fact, if your source data includes any information about effectivity or not. The next deciding factor is, whether you receive full loads of your data.

## USING REGULAR SATELLITES

Source data about effectivity can come in many shapes. Some examples are listed below:

- Start- & End-Dates
- CDC Information
- Effective-From & Effective-To Dates

In all those cases, we teach to treat those attributes just as regular descriptive attributes, and ignore the fact, that they include time ranges. Therefore you should just use the regular satellite version 0 & 1 macros.

What we also recommend, is to split all those effectivity attributes into a separate satellite for auditing purposes. Therefore your satellite version 0 model would only have all the effectivity attributes as a payload.

Any business logic that uses those effectivity ranges would be applied on top of these satellites and is out of datavault4dbt’s scope.

For further information regarding this topic, also called multi-temporality, see these Scalefree Blog Articles:

- Multi-Temporality in Data Vault 2.0 – Part 1
- Multi-Temporality in Data Vault 2.0 – Part 2

## USING EFFECTIVITY SATELLITES

If you receive full loads, effectivity satellites can be used to track appearances and disappearances of hashkeys. Check out the effectivity satellite documentation for more details!

## USING RECORD TRACKING SATELLITES

In case there is no effectivity data coming from the source and you don’t receive full loads, or even when you do not trust your source system and want to track the appearance of business keys or relations, record tracking satellites are here to help! The purpose of a record tracking satellite is, to track the appearance of a hashkey. That can either be a Hub hashkey, or a Link hashkey, and therefore a record tracking satellite can be attached both to Hubs and Links.

The record tracking satellite macro requires the name of a stage model, and the name of a hashkey column as an input. So if you want to track the appearance of a relationship, that relationship is most likely modeled as a Link and would therefore have a Link hashkey calculated in a stage model. That means, your record tracking satellite for that link should be based on the same staging model as the link, and the name of the Link hashkey should be tracked.

In the end, each record tracking satellite will hold one row per hashkey per load date that it appeared. This leads to the only downside of using record tracking satellites – they can grow really large over time.

In the future, we plan to integrate the same logarithmic logic that is used inside the snapshot table for PIT cleanup, to also clean up record tracking satellites and reducing the amount of data in them.

Of course, a record tracking satellite does not directly includes information about effectivity ranges for business keys or relationships. But since extracting those ranges out of appearances is business logic, they are highly depending on business requirements and hence out of scope for datavault4dbt. At least for the beginning!