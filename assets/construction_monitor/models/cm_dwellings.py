"""
# cm_surveydate.py
"""
import pandas as pd

def v0_0_2(permit_df: pd.DataFrame) -> pd.DataFrame:
    """calculate permit month from permit date."""
    df = permit_df.copy()

    df["permit_dwellings"] = df["PMT_UNITS"].fillna(0)

    return df[["permit_dwellings"]]

assets = {
    "field_name": "permit_dwellings",
    "active_versions": [v0_0_2]  # List of active versions for this asset
}
