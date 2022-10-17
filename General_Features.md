This package enforces a few features over all macros:

    - Ready for both Persistent Staging Areas and Transient Staging Areas, due to the allowance of multiple deltas in all macros, without losing any intermediate changes
    - Enforcing standards in naming conventions by implementing global variables for technical columns
    - Following the insert-only-approach by using a mix of tables and views
    - Creating a snapshot-based Business interface by using a centralized snapshot table supporting logarithmic logic
    - Optimizing incremental loads by implementing a high-water-mark that also works for entities that are loaded from multiple sources

