# KinetikIQ_Project_Revenue_Reconciler
In this project I am building a daily reporting pipeline for the Finance team. Our goal is to calculate the Daily Recurring Revenue (DRR) for the month of June 2023. Because their system is distributed, events often arrive out of order, and their pricing model includes "grandfathering" . The system receives duplicate logs due to network retries. Errors are handled with Try and except 

# Default run — June 2023, files in the same folder
python RevenueReconciler.py

# Custom date range
python RevenueReconciler.py --start 2023-06-01 --end 2023-06-30

# Custom file paths
python RevenueReconciler.py --events path/to/events.csv --plans path/to/plans.csv

# Fully custom
python RevenueReconciler.py --events path/to/events.csv --plans path/to/plans.csv --start 2023-05-01 --end 2023-05-31

# Daily Recurring Revenue (DRR)
DRR is the sum of the daily prices of all active subscriptions evaluated at the end of each day (23:59:59).

# Subscription State
A user is **active** if their most recent event (by `event_timestamp`) up to end of day is a `sub`. A user is **inactive** if their most recent event is an `unsub`, or if they have no events.

# Late Arrivals
Events are always sorted by `event_timestamp`, not by the order they appear in the file. If an `unsub` arrives with an earlier timestamp than a `sub`, the timestamps determine the final state — not the row order.

# Grandfathered Pricing
A user's price is locked in at the plan price that was in effect **at the exact second they first subscribed**. Price increases after that date do not affect existing subscribers.

# Deduplication
If an `event_id` appears more than once in the source data (due to network retries), only the first occurrence is counted. Duplicates are dropped before any processing.

Raw CSVs in the folder
   1. extract_data()                              — load and parse both CSV files
   2. dropduplicate_events()           — remove duplicate event_ids
   3. build_drr_report()              — loop over each day in range
         get_active_subscriptions_on_day()        — find active users at 23:59:59
               get_grandfathered_price()          — lock in price at first sub time
   4. Export CSV + print to terminal

Report saved to Revenue_report_2023-06-01_2023-06-30.csv
