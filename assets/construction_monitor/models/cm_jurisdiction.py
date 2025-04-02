"""
# cm_jurisdiction.py
"""
import pandas as pd

def v0_0_2(permit_df: pd.DataFrame) -> pd.DataFrame:
    """Assume that juris is exactly the same as in cm data."""
    df = permit_df.copy()

    df["jurisdiction"] = df["SITE_JURIS"].str.upper()
    df["state"] = df["SITE_STATE"].str.upper()

    df["state_fips"] = (
        df["SITE_STATE_FIPS"].astype(str) if "SITE_STATE_FIPS" in df.columns else ""
    )
    df["county_fips"] = (
        df["SITE_CNTY_FIPS"].astype(str) if "SITE_STATE_FIPS" in df.columns else ""
    )

    result_df = df[
        ["jurisdiction", "state", "state_fips", "county_fips"]
    ]
    return result_df


# def v0_0_3(permit_df: pd.DataFrame) -> pd.DataFrame:
#     """use fuzzy match against the AGF to find the best fit."""
#     df = permit_df.copy()

#     result_df = df["jurisdiction"]
#     return result_df


# def v0_0_4(permit_df: pd.DataFrame) -> pd.DataFrame:
#     """use permit geocoding against the AGF to find the best fit."""
#     df = permit_df.copy()

#     result_df = df["jurisdiction"]
#     return result_df


assets = {
    "field_name": "jurisdiction",
    "active_versions": [v0_0_2]  # List of active versions for this asset
}

