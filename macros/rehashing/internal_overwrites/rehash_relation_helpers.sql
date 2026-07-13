{#
    Helpers that keep the "_deprecated" copy of a rehashed entity consistent
    across the rename, the CREATE ... FROM, and the final DROP.

    Why this exists:
        The rehash macros used to derive the deprecated copy with
        make_temp_relation(rel, suffix='_deprecated'). make_temp_relation is
        allowed to add uniqueness suffixes to the identifier (dbt-bigquery
        >= 1.11.2 appends a %H%M%S%f timestamp), so the CREATE/DROP referenced a
        different identifier than the plain "<identifier>_deprecated" produced by
        the rename. The rehash then failed with
        "Table ..._deprecated<timestamp> was not found".

        rehash_deprecated_relation builds that identifier deterministically, and
        rehash_prepare_rename performs the rename idempotently so an interrupted
        prior run no longer wedges every following run with a 409 / "Already
        Exists" or a leftover orphan.

    Both helpers rely only on dbt built-ins (load_relation, run_query,
    drop_table, get_rename_table_sql, Relation.incorporate), so they are
    adapter-agnostic.
#}

{# Stable "<identifier>_deprecated" relation -- never routed through make_temp_relation. #}
{% macro rehash_deprecated_relation(relation) %}
    {{ return(relation.incorporate(path={"identifier": relation.identifier ~ '_deprecated'})) }}
{% endmacro %}


{# Rename `relation` to its _deprecated copy, recovering safely from an interrupted prior run.
   Returns the deprecated relation so callers can reuse it for the CREATE FROM / DROP. #}
{% macro rehash_prepare_rename(relation, output_logs=true) %}

    {% set deprecated = datavault4dbt.rehash_deprecated_relation(relation) %}

    {% if load_relation(deprecated) is not none %}
        {% if load_relation(relation) is not none %}
            {# Both exist: the _deprecated copy is a stale orphan, live data is in `relation`. #}
            {{ log('Dropping stale deprecated table left by a previous run: ' ~ deprecated, output_logs) }}
            {% do run_query(drop_table(deprecated)) %}
        {% else %}
            {# Only the _deprecated copy exists: a prior run died between rename and create. Recover it. #}
            {{ log('Recovering ' ~ relation.identifier ~ ' from orphaned ' ~ deprecated.identifier ~ ' (interrupted prior run).', output_logs) }}
            {% do run_query(get_rename_table_sql(deprecated, relation.identifier)) %}
        {% endif %}
    {% endif %}

    {% do run_query(get_rename_table_sql(relation, deprecated.identifier)) %}

    {{ return(deprecated) }}

{% endmacro %}
