---
sidebar_position: 34
sidebar_label: PostgreSQL
title: PostgreSQL
---

# POSTGRESQL

---

## SATELLITE SPLITS

By default, PostgreSQL sets the Function Arguments (MAX_FUNCTION_ARGUMENTS) to 100, which imposes a limitation of 50 columns per satellite when the server sticks to default settings. To accommodate more columns for a satellite in PostgreSQL, you’ll need to adjust the database server settings or opt for more frequent satellite splits. This adjustment becomes necessary to manage larger sets of columns effectively within the satellites.