{#
    This macro creates a view that extends an existing control_snap_v0 table by dynamically changing information.
    These information include a logic to implement logarithmic snapshots. That means that the further i look into 
    the past, the more coarsely the snapshots should be granulated. For example i want to keep daily snapshots
    for the past 30 days, but i am not interested in daily snapshots for the past 10 years, and therefor i only
    keep weekly snaphots for the past 6 months, monthly snapshots for the past 3 years, and yearly snapshots for
    ever. This procedure strongly reduces the number of active snapshots, and therefor also the number of rows,
    and the required computation inside all PITs and Bridges. This logic is optional and would be captured in a
    boolean column called 'is_active'.

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
                                            The units include: 

                                        


#}