"""
______________________________________________________________________________________
Temporal Revenue Reconciler 
KinetikIQ Project - April 25 2026
Script Name: RevenueReconciler.py
______________________________________________________________________________________
This script calculates Daily Recurring Revenue (DRR).

Objectives:
    - Drop duplicate events (event_id is unique))
    - Out-of-order and late-arriving events handled by sorting by event_timestamp
    - Grandfathered pricing (price locked at original subscription time)
    - Users who only have an unsub (never counted as active)
    - Dynamic date range (not hardcoded to default date June 2023)
    - Dynamic file paths for input CSVs (not hardcoded)
    - Basic error handling for missing files, malformed data, and date parsing issues

    How to use:
    # Use defaults (June 2023, files saved in the same folder)
     
    # Default date range
    python revenueReconciler.py --start 2023-06-01 --end 2023-06-30
 
    # Custom output filename
    drr_report_june_2023.csv 
"""

import os
import argparse
import pandas as pd
from datetime import date, timedelta

# ---------------------------------------------------------------------------
# 1. Extract data
# ---------------------------------------------------------------------------

def extract_data(user_events_path: str, plans_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load and parse dates in the raw CSV files."""

    for path in (user_events_path, plans_path):
        if not os.path.exists(path):
            raise FileNotFoundError(f"Required file not found: {path}")

    try:
        events_df = pd.read_csv(user_events_path, parse_dates=["event_timestamp"])
    except Exception as exc:
        raise ValueError(f"Failed to load user events from {user_events_path}: {exc}") from exc

    try:
        plans_df = pd.read_csv(plans_path, parse_dates=["effective_date"])
    except Exception as exc:
        raise ValueError(f"Failed to load plan data from {plans_path}: {exc}") from exc


    return events_df, plans_df

# ---------------------------------------------------------------------------
# 2. Transform Data - Drop duplicate records considering event_id as a unique identifier,
#    keeping the first occurrence.
# ---------------------------------------------------------------------------

def dropduplicate_events(events_df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop duplicate event_ids and keep only the first occurrence.
    """
    events_df = events_df.drop_duplicates(subset="event_id", keep="first")
    return events_df

# ---------------------------------------------------------------------------
# 3. Grandfathered price lookup
# ---------------------------------------------------------------------------

def get_grandfathered_price(
    plan_id: str,
    sub_timestamp: pd.Timestamp,
    plans_df: pd.DataFrame,
) -> float:
    """
    Return the plan price that was in effect at the moment of subscription.
    Find all price rows for this plan whose effective_date <= sub_timestamp,
    then take the most recent one (the highest effective_date in that set).
    """
    eligible = plans_df[
        (plans_df["plan_id"] == plan_id) &
        (plans_df["effective_date"] <= sub_timestamp)
    ]
    if eligible.empty:
        raise ValueError(
            f"No price found for plan '{plan_id}' at {sub_timestamp}"
        )
    return eligible.sort_values("effective_date").iloc[-1]["price"]

# ---------------------------------------------------------------------------
# 4. Determine active users and their locked-in price for a single day
# ---------------------------------------------------------------------------

def get_active_subscriptions_on_day(
    day_end: pd.Timestamp,
    events_df: pd.DataFrame,
    plans_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    For a given day (EOD at 23:59:59), return a DataFrame of
    active users with columns: user_id, plan_id, locked_price.

    A user is active if:
      - They have at least one event up to day_end, AND their most recent event (by timestamp) is a 'sub'
    """
    # Only consider events that have arrived by end of this day
    day_events = events_df[events_df["event_timestamp"] <= day_end].copy()

    if day_events.empty:
        return pd.DataFrame(columns=["user_id", "plan_id", "locked_price"])

    # For each user, find their latest event
    latest = (
        day_events
        .sort_values("event_timestamp")
        .groupby("user_id")
        .last()
        .reset_index()
    )

    # Keep only users whose latest event is a subscription
    active = latest[latest["event_type"] == "sub"].copy()

    if active.empty:
        return pd.DataFrame(columns=["user_id", "plan_id", "locked_price"])

    # For each active user, find their original subscription timestamp
    # (the earliest 'sub' event) to lock in their grandfathered price.
    # We are looking at all events up to day_end, not just their latest event.
    first_subs = (
        day_events[day_events["event_type"] == "sub"]
        .sort_values("event_timestamp")
        .groupby("user_id")
        .first()
        .reset_index()[["user_id", "plan_id", "event_timestamp"]]
        .rename(columns={"event_timestamp": "first_sub_ts"})
    )

    # Rename plan_id in first_subs to avoid collision on merge
    first_subs = first_subs.rename(columns={"plan_id": "original_plan_id"})
    active = active[["user_id", "plan_id"]].merge(first_subs[["user_id", "original_plan_id", "first_sub_ts"]], on="user_id", how="left")

    # Look up the locked-in price for each user using their original plan + sub time
    active["locked_price"] = active.apply(
        lambda row: get_grandfathered_price(row["original_plan_id"], row["first_sub_ts"], plans_df),
        axis=1,
    )

    return active[["user_id", "plan_id", "locked_price"]]

# ---------------------------------------------------------------------------
# 5. Load - Build the full report - June 2023 default report
# ---------------------------------------------------------------------------

def build_drr_report(
    events_df: pd.DataFrame,
    plans_df: pd.DataFrame,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """
    Produce a daily DRR report for every day from start_date to end_date.
    Returns a DataFrame with columns: date, active_users, total_daily_revenue.
    """
    rows = []
    current = start_date

    while current <= end_date:
        # Evaluate state at the end of the day
        day_end = pd.Timestamp(current) + pd.Timedelta(hours=23, minutes=59, seconds=59)

        active_subs = get_active_subscriptions_on_day(day_end, events_df, plans_df)

        rows.append({
            "date": current.strftime("%Y-%m-%d"),
            "active_users": len(active_subs),
            "total_daily_revenue": round(active_subs["locked_price"].sum(), 2),
        })

        current += timedelta(days=1)

    return pd.DataFrame(rows)

# ---------------------------------------------------------------------------
# 6. Main
# ---------------------------------------------------------------------------

def main():

# Use argparse to allow dynamic file paths and date range

    parser = argparse.ArgumentParser(description="Daily Revenue Reconciler")
    parser.add_argument("--events", default="user_events.csv",  help="Path to events CSV")
    parser.add_argument("--plans",  default="plans.csv",        help="Path to plans CSV")
    parser.add_argument("--start",  default="2023-06-01",       help="Start date YYYY-MM-DD")
    parser.add_argument("--end",    default="2023-06-30",       help="End date YYYY-MM-DD")
    args = parser.parse_args()

    start_date = date.fromisoformat(args.start)
    end_date   = date.fromisoformat(args.end)

    print("Loading data...")
    events_df, plans_df = extract_data(args.events, args.plans)
    #print(events_df.head())  # for testing the loaded data, can be commented out in final version.
    #print(plans_df.head())
    print("Data loaded.")

    print("Dropping duplicate events...")
    events_df = dropduplicate_events(events_df)
    #print(events_df)
    print("Duplicate events removed if exists.")

    print("Building DRR report for June 2023...\n")
    report = build_drr_report(events_df, plans_df, start_date, end_date)

    # Display
    pd.set_option("display.float_format", "{:.2f}".format)
    print(report.to_string(index=False))

    # Load the report to a CSV file
    report.to_csv(f"Revenue_report_{start_date.strftime('%Y-%m')}.csv", index=False)
    print(f"\nReport saved to Revenue_report_{start_date.strftime('%Y-%m')}.csv")

if __name__ == "__main__":
    main()