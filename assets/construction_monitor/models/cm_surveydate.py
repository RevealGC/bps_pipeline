"""
# cm_surveydate.py
"""

import pandas as pd


def v0_0_2(permit_df: pd.DataFrame) -> pd.DataFrame:
    """calculate permit month from permit date."""
    result_df = permit_df.copy()
    result_df["permit_month"] = pd.to_datetime(result_df["PMT_DATE"]).dt.strftime("%Y%m")
    return result_df[["permit_month"]]

assets = {
    "field_name": "permit_month",
    "active_versions": [v0_0_2]  # List of active versions for this asset
}
