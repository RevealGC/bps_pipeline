"""cm_transform.py """

import pandas as pd
import dagster as dg

from assets.construction_monitor.cm_helper import assign_unit_group
from assets.construction_monitor.models.cm_model_asset_factory import (
    build_all_model_assets
)
from assets.construction_monitor.cm_csv_files import (
    cm_permit_files_partitions,
)
from assets.construction_monitor.models import (
    cm_surveydate,
    cm_jurisdiction,
    cm_unitgroup,
    cm_dwellings,
    cm_cct,
    )

shared_params = {
    "partitions_def": cm_permit_files_partitions,
    "ins": {"permit_df": dg.AssetIn(dg.AssetKey(["cm_permit_files"]))},
    "io_manager_key": "parquet_io_manager",
    "group_name": "cm_permits",
    "owners": ["elo.lewis@revealgc.com", "team:construction-reengineering"],
    "automation_condition": dg.AutomationCondition.eager(),
}

assets_params = [
    cm_surveydate.assets,
    cm_unitgroup.assets,
    cm_jurisdiction.assets,
    cm_dwellings.assets,
    cm_cct.assets,
]

def cm_modeled_assets():
    """
    Create versioned assets
    NOTE: asset params removed for reuse by agg functions
    unnessary wrapper legacy - refactor
    """
    return build_all_model_assets(assets_params, shared_params)


# # still needed for aggregates
# @dg.asset(
#     **shared_params,
# )
# def calculate_permit_month(permit_df: pd.DataFrame):
#     """
#     calculate permit month from permit date.
#     This is a placeholder to ensure cm_modeled_assets() can be called.
#     """
#     result_df = permit_df.copy()
#     result_df["permit_month"] = pd.to_datetime(result_df["PMT_DATE"]).dt.strftime("%Y%m")
#     return dg.Output(
#         result_df[["permit_month"]],
#         metadata={
#             "num_rows": result_df.shape[0],
#             "num_columns": 1,
#             "preview": dg.MetadataValue.md(result_df.head().to_markdown()),
#         },
#     )   


# # calculate_jurisdiction.py
# # asset for jurisdiction assignment
# @dg.asset(
#     **shared_params,
#     description="calculate permit month from permit date",
#     code_version="0.0.3",
# )
# def calculate_jurisdiction(permit_df: pd.DataFrame):
#     """calculate permit jurisdiction from site jurisdiction."""
#     df = permit_df.copy()
#     df["jurisdiction"] = df["SITE_JURIS"].str.upper()
#     df["state"] = df["SITE_STATE"].str.upper()
#     # site state fips is not always available

#     df["state_fips"] = (
#         df["SITE_STATE_FIPS"].astype(str) if "SITE_STATE_FIPS" in df.columns else ""
#     )
#     df["county_fips"] = (
#         df["SITE_CNTY_FIPS"].astype(str) if "SITE_STATE_FIPS" in df.columns else ""
#     )

#     code_version = "0.0.2"
#     df["code_version_juris"] = code_version

#     permit_df = df[
#         ["jurisdiction", "state", "state_fips", "county_fips", "code_version_juris"]
#     ]

#     return dg.Output(
#         permit_df,
#         metadata={
#             "num_rows": permit_df.shape[0],
#             "num_columns": permit_df.shape[1],
#             "preview": dg.MetadataValue.md(permit_df.head().to_markdown()),
#         },
#     )


# calculate_unit_group.py
# asset for unit_group assignment
# @dg.asset(
#     **shared_params,
#     description="calculate permit unit group",
#     code_version="0.0.2",
# )
# def calculate_unit_group(permit_df: pd.DataFrame):
#     """calculate permit unit group from permit units."""
#     df = permit_df.copy()
#     df["PMT_UNITS"] = pd.to_numeric(df["PMT_UNITS"], errors="coerce")
#     df["unit_group"] = df["PMT_UNITS"].apply(assign_unit_group)

#     code_version = "0.0.2"
#     df["code_version_unitgroups"] = code_version

#     permit_df = df[["unit_group", "code_version_unitgroups"]]

#     return dg.Output(
#         permit_df,
#         metadata={
#             "num_rows": permit_df.shape[0],
#             "num_columns": permit_df.shape[1],
#             "preview": dg.MetadataValue.md(permit_df.head().to_markdown()),
#         },
#     )


# # asset for dwellings imputation
# @dg.asset(
#     **shared_params,
#     description="calculate permit dwellings",
#     code_version="0.0.2",
# )
# def impute_dwellings(permit_df: pd.DataFrame):
#     """impute permit dwellings from permit units."""
#     df = permit_df.copy()
#     df["permit_dwellings"] = df["PMT_UNITS"].fillna(0)
#     code_version = "0.0.2"
#     df["code_version_dwellings"] = code_version

#     permit_df = df[["permit_dwellings", "code_version_dwellings"]]

#     return dg.Output(
#         permit_df,
#         metadata={
#             "num_rows": permit_df.shape[0],
#             "num_columns": permit_df.shape[1],
#             "preview": dg.MetadataValue.md(permit_df.head().to_markdown()),
#         },
#     )
