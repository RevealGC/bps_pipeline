"""
# cm_unitgroup.py
"""

import pandas as pd
from assets.construction_monitor.cm_helper import assign_unit_group


def v0_0_2(permit_df: pd.DataFrame) -> pd.DataFrame:
    """calculate permit month from permit date."""
    df = permit_df.copy()
    
    df["PMT_UNITS"] = pd.to_numeric(df["PMT_UNITS"], errors="coerce")
    df["unit_group"] = df["PMT_UNITS"].apply(assign_unit_group)

    result_df = df[["unit_group"]]

    return result_df

assets = {
    "field_name": "unit_group",  # The field name in the output DataFrame
    "active_versions": [v0_0_2]  # List of active versions for this asset
}
