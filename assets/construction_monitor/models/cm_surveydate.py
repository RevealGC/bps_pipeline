"""
# cm_surveydate.py
"""

import pandas as pd


def v0_0_2(permit_df: pd.DataFrame) -> pd.DataFrame:
    """calculate permit month from permit date."""
    df = permit_df.copy()
    df["permit_month"] = pd.to_datetime(df["PMT_DATE"]).dt.strftime("%Y%m")
    return df[["permit_month"]]

assets = {
    "field_name": "permit_month",
    "active_versions": [v0_0_2]  # List of active versions for this asset
}
