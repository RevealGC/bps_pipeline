"""
d
"""

# from datetime import datetime
import os
import duckdb
import pandas as pd
from dagster import (
    AssetIn,
    Output,
    AssetKey,
    AutomationCondition,
    AssetExecutionContext,
    asset,
)

from assets.construction_monitor.cm_csv_files import cm_permit_files_partitions
from assets.construction_monitor.cm_aggregate import evaluations_partitions_def
from utilities.dagster_utils import get_outpath


# @asset(
#     io_manager_key="parquet_io_manager",
#     ins={
#         "cm": AssetIn(AssetKey(["aggregate_permit_models"])),
#     },
#     deps=[AssetKey("bps_survey_files")],
#     group_name="cm_permits",
#     description="join permit data with BPS survey data",
#     partitions_def=cm_permit_files_partitions,
#     automation_condition=AutomationCondition.eager(),
# )
# def compose_permit_models(
#     context: AssetExecutionContext,
#     cm: pd.DataFrame,
# ) -> Output[pd.DataFrame]:
#     """
#     Join permit data with BPS survey data.
#     """
#     cm_file = context.partition_key

#     # context.log.info(f"Expected asset key: {AssetKey('bps_survey_files')}")
#     # for an_asset in materialized_assets:
#     #     context.log.info(f"Materialized Asset: {an_asset.to_string()}"
#     asset_key = AssetKey(["bps_survey_files"])
#     outpath = get_outpath(context, asset_key)
#     # context.log.info(f"outpath: {outpath}")
#     # outpath = outpath.replace("{partition_key}", "*")
#     # outpath = f"data/{outpath}"

#     query = f"""
#         SELECT *
#         FROM read_parquet('{outpath}')
#         WHERE measure = 'Units'
#         """
#     bps = duckdb.sql(query).to_df()

#     bps["Place_Name_upper"] = bps["Place_Name"].str.upper()

#     merge_keys = [
#         ("jurisdiction", "Place_Name_upper"),
#         ("unit_group", "unit_group"),
#         ("permit_month", "Survey_Date"),
#     ]

#     # Add state_fips to merge keys if available in both DataFrames
#     if "State_Code" in cm.columns:
#         merge_keys.append(("state_fips", "State_Code"))

#     # Add county_fips to merge keys if available in both DataFrames
#     if "County_Code" in cm.columns:
#         merge_keys.append(("county_fips", "County_Code"))

#     # Perform the join using the combined condition
#     overlap = cm.merge(
#         bps,
#         how="inner",
#         left_on=[key[0] for key in merge_keys],
#         right_on=[key[1] for key in merge_keys],
#     )
#     overlap["cm_file"] = cm_file

#     return Output(
#         overlap,
#         metadata={
#             "num_rows": overlap.shape[0],
#             "num_columns": overlap.shape[1],
#             "preview": overlap.head().to_markdown(),
#         },
#     )
