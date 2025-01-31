"""cm_transform.py """

import pandas as pd
from dagster import (
    Output,
    AssetIn,
    AssetKey,
    MetadataValue,
    AutomationCondition,
    asset,
)

from assets.construction_monitor.cm_csv_files import (
    cm_permit_files_partitions,
)

shared_params = {
    "partitions_def": cm_permit_files_partitions,
    "ins": {"df": AssetIn(AssetKey(["cm_permit_files"]))},
    "io_manager_key": "parquet_io_manager",
    "group_name": "cm_permits",
    "owners": ["elo.lewis@revealgc.com", "team:construction-reengineering"],
    "automation_condition": AutomationCondition.eager(),
}


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


# calculate_permit_month.py
# asset for permit month assignment
@asset(
    **shared_params,
    description="calculate permit month from permit date",
    code_version="0.0.2",
)
def calculate_permit_month(df: pd.DataFrame):
    """calculate permit month from permit date."""
    df["permit_month"] = pd.to_datetime(df["PMT_DATE"]).dt.strftime("%Y%m")

    code_version = "0.0.2"
    df["code_version_months"] = code_version

    permit_df = df[["permit_month", "code_version_months"]]

    return Output(
        permit_df,
        metadata={
            "num_rows": permit_df.shape[0],
            "num_columns": permit_df.shape[1],
            "preview": MetadataValue.md(permit_df.head().to_markdown()),
        },
    )


# calculate_jurisdiction.py
# asset for jurisdiction assignment
@asset(
    **shared_params,
    description="calculate permit month from permit date",
    code_version="0.0.3",
)
def calculate_jurisdiction(df: pd.DataFrame):
    """calculate permit jurisdiction from site jurisdiction."""
    df["jurisdiction"] = df["SITE_JURIS"].str.upper()
    df["state"] = df["SITE_STATE"].str.upper()
    # site state fips is not always available

    df["state_fips"] = (
        df["SITE_STATE_FIPS"].astype(str) if "SITE_STATE_FIPS" in df.columns else ""
    )
    df["county_fips"] = (
        df["SITE_CNTY_FIPS"].astype(str) if "SITE_STATE_FIPS" in df.columns else ""
    )

    code_version = "0.0.2"
    df["code_version_juris"] = code_version

    permit_df = df[
        ["jurisdiction", "state", "state_fips", "county_fips", "code_version_juris"]
    ]

    return Output(
        permit_df,
        metadata={
            "num_rows": permit_df.shape[0],
            "num_columns": permit_df.shape[1],
            "preview": MetadataValue.md(permit_df.head().to_markdown()),
        },
    )


# calculate_unit_group.py
# asset for unit_group assignment
@asset(
    **shared_params,
    description="calculate permit unit group",
    code_version="0.0.2",
)
def calculate_unit_group(df: pd.DataFrame):
    """calculate permit unit group from permit units."""
    df["PMT_UNITS"] = pd.to_numeric(df["PMT_UNITS"], errors="coerce")
    df["unit_group"] = df["PMT_UNITS"].apply(assign_unit_group)

    code_version = "0.0.2"
    df["code_version_unitgroups"] = code_version

    permit_df = df[["unit_group", "code_version_unitgroups"]]

    return Output(
        permit_df,
        metadata={
            "num_rows": permit_df.shape[0],
            "num_columns": permit_df.shape[1],
            "preview": MetadataValue.md(permit_df.head().to_markdown()),
        },
    )


# asset for dwellings imputation
@asset(
    **shared_params,
    description="calculate permit dwellings",
    code_version="0.0.2",
)
def impute_dwellings(df: pd.DataFrame):
    """impute permit dwellings from permit units."""
    df["permit_dwellings"] = df["PMT_UNITS"].fillna(0)
    code_version = "0.0.2"
    df["code_version_dwellings"] = code_version

    permit_df = df[["permit_dwellings", "code_version_dwellings"]]

    return Output(
        permit_df,
        metadata={
            "num_rows": permit_df.shape[0],
            "num_columns": permit_df.shape[1],
            "preview": MetadataValue.md(permit_df.head().to_markdown()),
        },
    )
