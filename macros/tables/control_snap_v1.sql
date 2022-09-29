{#
    This macro creates a view that extends an existing control_snap_v0 table by dynamically changing information.
    These information include a logic to implement logarithmic snapshots. That means that the further i look into
    the past, the more coarsely the snapshots should be granulated. For example i want to keep daily snapshots
    for the past 30 days, but i am not interested in daily snapshots for the past 10 years, and therefor i only
    keep weekly snaphots for the past 6 months, monthly snapshots for the past 3 years, and yearly snapshots for
    ever. This procedure strongly reduces the number of active snapshots, and therefor also the number of rows,
    and the required computation inside all PITs and Bridges. This logic is optional and would be captured in a
    boolean column called 'is_active'. It is overwritten by the force_active column in the v0 snapshot table.
    If a sdts is deactivated there, the log_logic does not reactivate it.

    Whenever a logarithmic snapshot logic is used and picked up by PIT tables, a logic is required that deletes
    records out of PIT tables, that are no longer active. For this a post_hook called "clean_up_pit" is provided
    in this package, that should be applied for each PIT table.

    In addition to that, a few other dynamic columns are generated:

        is_latest::boolean                  Captures if a sdts is the latest one inside the snapshot table. There
                                            is always only one snapshot inside the view, that has TRUE here.

        is_current_year::boolean            Captures if a sdts is part of the current calender year.

        is_last_year::boolean               Captures if a sdts is part of the last calender year.

        is_rolling_year::boolean            Captures if a sdts is inside the past year, starting from the current date.

        is_last_rolling_year::boolean       Captures if a sdts is inside the range that starts two years ago (from the
                                            current date) and ranges until one year ago (from the current date).

    Parameters:

        control_snap_v0::string             The name of the underlying version 0 control snapshot table. Needs to be
                                            available as a dbt model.

        log_logic::dictionary               Defining the desired durations of each granularity. Available granularities
                                            are 'daily', 'weekly', 'monthly', and 'yearly'. For each granularity the
                                            duration can be defined as an integer, and the time unit for that duration.
                                            The units include (in BigQuery): DAY, WEEK, MONTH, QUARTER, YEAR. Besides
                                            defining a duration and a unit for each granularity, there is also the option
                                            to set a granularity to 'forever'. E.g. reporting requires daily snapshots
                                            for 3 months, and after that the monthly snapshots should be kept forever.

                                            If log_logic is not set, no logic will be applied, and all snapshots will stay
                                            active. The other dynamic columns are calculated anyway.

                                            The duration is always counted from the current date.

                                            Examples:
                                                {'daily': {'duration': 3,               This configuration would keep daily
                                                            'unit': 'MONTH',            snapshots for 3 months, weekly snapshots
                                                            'forever': 'FALSE'},          for 1 year, monthly snapshots for 5
                                                'weekly': {'duration': 1,               years and yearly snapshots forever.
                                                            'unit': 'YEAR'},            If 'forever' is not defined here, it
                                                'monthly': {'duration': 5,              is automatically set to 'FALSE'.
                                                            'unit': 'YEAR'},            Therefor it could have been left out
                                                'yearly': {'forever': 'TRUE'} }         in the configurtaion for daily snapshots.

                                                {'daily': {'duration': 90,              This would keep daily snapshots for 90
                                                           'unit': 'DAY'},              days, and monthly snapshots forever.
                                                 'monthly': {'forever': 'TRUE'}}

#}

{%- macro control_snap_v1(control_snap_v0, log_logic=none) -%}

{{ return(adapter.dispatch('control_snap_v1', 'datavault4dbt')(control_snap_v0=control_snap_v0,
                                                                    log_logic=log_logic)) }}

{%- endmacro -%}
