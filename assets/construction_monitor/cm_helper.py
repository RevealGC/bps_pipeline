"""
cm_helper.py
Helper functions for construction monitor ETL.
"""


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
