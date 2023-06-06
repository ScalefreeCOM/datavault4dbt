{#
    This macro creates a snapshot table to control snapshot-based tables like PITs and Bridges. The snapshot
    table will hold daily snapshots starting at a specific start_date and has a configurable daytime.
    Usual application would involve creating one snapshot table per Data Vault environment, and therefore creating
    one dbt model using this macro. The model needs to be scheduled daily, with an execution time that matches your
    desired snapshot time, since the macro automatically inserts all snapshots until the current day, no matter which
    time it is, and a snapshot for 08:00:00 should be calculated at that time. So if the snapshot table is configured
    to have a 'daily_snapshot_time' of '07:00:00', all snapshots in the table will have the timestamp '07:00:00'.
    Therefore you need to schedule the building of the snapshot table also to '07:00:00'.
    In addition to the actual snapshot-datetimestamp (sdts), the macro generates the following columns:

        replacement_sdts::timestamp     Allows users to replace a sdts with another one, without having to update the actual sdts column.
                                        By default this column is filled with the regular sdts.

        caption::string                 Allows users to title their snapshots. Examples would be something like: 'Christmas 2022', or
                                        'End-of-year report 2021'. By default this is filled with 'Snapshot {sdts}', holding the
                                        respective sdts.

        is_hourly::boolean              Captures if the time of a sdts is on exact hours, meaning minutes=0 and seconds=0. All sdts
                                        created by this macro are daily and therefore always hourly, but this column enables future inserts
                                        of custom, user-defined sdts.

        is_daily::boolean               Captures if the time of a sdts is on exactly midnight, meaning hours=0, minutes=0 and seconds=0.
                                        This depends on your desired daily_snapshot_time, but is not used by the downstream macros, and
                                        just generates additional metadata for potential future use.

        is_weekly::boolean              Captures if the day of the week of a sdts is Monday.

        is_monthly::boolean             Captures if a sdts is the first day of a month.

        is_yearly::boolean              Captures if a sdts is the first day of a year.

        comment::string                 Allows users to write custom comments for each sdts. By default this column is set to NULL.
        
        force_active::boolean           Allows users to deactivate single snapshots. Deactivating a snapshot here overwrites any logarithmic
                                        logic that is applied in the version 1 snapshot table on top of this one. This column is automatically
                                        set to TRUE.

        force_active::boolean           Allows users to deactivate single snapshots. Deactivating a snapshot here overwrites any logarithmic
                                        logic that is applied in the version 1 snapshot table on top of this one. This column is automatically
                                        set to TRUE.

    Parameters:

        start_date::timestamp           Defines the earliest timestamp that should be available inside the snapshot_table. The time part of this
                                        timestamp needs to be set to '00:00:00'. The format of this timestamp must equal to the timestamp format
                                        defined in the global variable 'datavault4dbt.timestamp_format'.

                                        Examples:
                                            '2015-01-01T00-00-00'   This snapshot table would hold daily snapshots beginning at 2015.

        daily_snapshot_time::time       Defines the time that your daily snapshots should have. Usually this is either something right before
                                        daily business starts, or after daily business is over.

                                        Examples:
                                            '07:30:00'      The snapshots inside this table would all have the time '07:30:00'.
                                            '23:00:00'      The snapshots inside this table would all have the time '23:00:00'.
                                                    
        sdts_alias::string              Defines the name of the snapshot date timestamp column inside the snapshot_table. It is optional,
                                        if not set will use the global variable `datavault4dbt.sdts_alias` set inside dbt_project.yml

#}

{%- macro control_snap_v0(start_date, daily_snapshot_time, sdts_alias=none, end_date=none) -%}
    
    {%- set sdts_alias = datavault4dbt.replace_standard(sdts_alias, 'datavault4dbt.sdts_alias', 'sdts') -%}

    {{ adapter.dispatch('control_snap_v0', 'datavault4dbt')(start_date=start_date,
                                                            daily_snapshot_time=daily_snapshot_time,
                                                            sdts_alias=sdts_alias,
                                                            end_date=end_date) }}

{%- endmacro -%}
