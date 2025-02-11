"""
cm_helper.py
Helper functions for construction monitor ETL.
"""

import re
from datetime import datetime
import pandas as pd


def assign_unit_group(units):
    """Assign permit unit group based on number of units."""
    if units == 1:
        return "1_unit"
    elif units == 2:
        return "2_units"
    elif 3 <= units <= 4:
        return "3_4_units"
    elif units >= 5:
        return "5+_units"
    elif units == 0:
        return "0"
    else:
        return "unknown"  # For unexpected values like 0 or negative


def parse_cm_date(cm_file):
    """
    Parses cm_filename to extract either:
    - Year-Month-Day (YYYY_MM_DD) -> Convert to YYYY-MM
    - Year-Week (YYYY_WW) -> Convert to the first day of that week, then to YYYY-MM
    """
    match_date = re.search(r"(\d{4}_\d{2}_\d{2})", cm_file)  # Match YYYY_MM_DD
    match_week = re.search(r"(\d{4}_\d{2})", cm_file)  # Match YYYY_WW

    if match_date:
        return pd.to_datetime(match_date.group(1), format="%Y_%m_%d", errors="coerce")

    elif match_week:
        year, week = map(int, match_week.group(1).split("_"))
        return pd.to_datetime(
            f"{year}-W{week}-1", format="%Y-W%W-%w", errors="coerce"
        )  # First day of that week

    return pd.NaT  # If no match, return NaT


# %%
def rename_cm_weekly_file(filename: str) -> str:
    """Rename cm_weekly file to match the format of cm_monthly file."""

    pattern = (
        r"(?P<company>[a-zA-Z]+)-(?P<type>[a-zA-Z]+)-(?P<year>\d{4})-(?P<week>\d+)"
    )
    match = re.match(pattern, filename)

    if not match:
        return filename  # Pattern didn't match, return original

    year = int(match.group("year"))
    week_number = int(match.group("week"))

    first_day = datetime.strptime(f"{year}-W{week_number}-1", "%G-W%V-%u")
    date_str = first_day.strftime("%Y_%m_%d")

    # Build the new filename
    new_filename = f"cm_orig_{date_str}.csv"
    return new_filename
