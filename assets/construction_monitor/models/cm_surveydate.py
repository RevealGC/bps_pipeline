"""
# cm_surveydate.py
"""

import pandas as pd


def v0_0_2(permit_df: pd.DataFrame) -> pd.DataFrame:
    """calculate permit month from permit date."""
    result_df = permit_df.copy()
    result_df["permit_month"] = pd.to_datetime(result_df["PMT_DATE"]).dt.strftime("%Y%m")
    return result_df[["permit_month"]]


def assets(shared_params) -> list:
    """Create versioned assets for permit month field."""

    field_name = "permit_month"
    active_versions = [v0_0_2]  # List of active versions for this asset

    model_assets = []
    for version in active_versions:
        code_version_str = version.__name__.replace("_", ".")
        model_assets.append(
            {
                "model_field_name": field_name,
                "version": code_version_str,
                "asset_params": {
                    **shared_params,
                    "description": version.__doc__,
                },
                "asset_func": version,
            }
        )

    return model_assets

