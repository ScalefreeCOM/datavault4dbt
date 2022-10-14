This package enforces a few features over all macros:

    - Ready for both Persistent Staging Areas and Transient Staging Areas, due to the allowance of multiple deltas in all macros, without losing any intermediate changes
    - Enforcing standards in naming conventions by implementing global variables for technical columns
    - Following the insert-only-approach by using a mix of tables and views
    - Creating a centralized, snapshot-based Business interface by using a centralized snapshot table supporting logarithmic logic
    - Optimizing incremental loads by implementing a high-water-mark that also works for entities that are loaded from multiple sources

Explaining the rsrc_static attribute:

    - Applies to Hubs, Links and NH-Links
    - Important when loading an entity from more than one source
    - For performance optimization of incremental loads, typically you only want to look at those records of the stage,
      that are newer than the maximum load date inside the target entity. This behaviour especially improves performance,
      if the underlying staging areas hold a large number of historical loads, since the number of rows scanned is 
      drastically reduced.
    - If an entity is loaded from multiple sources, each source should be filtered down to records, that are newer than
      the maximum load date of this exact source, not the overall maximum load date inside the target entity.
    - To allow that behaviour, the maximum load date for each different source needs to be calculated.
    - Sometimes the record source column inside one staging area is not always the same, but has some dynamic parts in it.
      Sometimes the rsrc column includes the ldts of each load and could look something like this: 'SALESFORCE/Partners/2022-01-01T07:00:00'. 
      Obviously the timestamp part inside that rsrc would change from load to load, and we now need to identify parts of
      it that will be static over all loads. In the shown example it would be 'SALESFORCE/Partners'. That expression now needs
      to be enriched with wildcard expressions to catch all occurrences of the static part of the record source. In BigQuery the
      wildcard would be '*' and therefore the rsrc_static would be '*/SALESFORCE/Partners/*'.
    - If the record source column of a staging area has no dynamic part and hence is always the same over all rows of a
      specific staging area, rsrc_static needs to be set to that whole static part, without wildcards.
    - Sometimes multiple different wildcard expressions belong to the same record source. In that case, rsrc_static can also
      be set to a list of wildcard expressions to catch all cases that belong together.
    - Sometimes the record source column of a stage has no static part, and the rows can not be grouped together again. In such
      a case rsrc_static is not to be set at all. Downside is, that the loading procedures do not benefit from the performance
      boost provided by the rsrc_static parameter. We recommend re-evaluating the contents of the record source column in such a case, to
      create at least a partially static content.

