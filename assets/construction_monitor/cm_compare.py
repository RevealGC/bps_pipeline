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
    EventRecordsFilter,
    AssetExecutionContext,
    DagsterEventType,
    asset,
)

from assets.construction_monitor.cm_csv_files import (
    cm_permit_partitions_def,
)


def get_outpath(context: AssetExecutionContext, asset_key: AssetKey) -> str:
    """Get the outpath of the upstream asset."""
    event_records = context.instance.event_log_storage.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.ASSET_MATERIALIZATION,
            asset_key=asset_key,
        ),
        limit=1,
    )
    if len(event_records) == 0:
        raise RuntimeError("Asset materialization event not found.")
    # context.log.info(
    #     "Using materialization event from run %s",
    #     event_records[0].event_log_entry.run_id,
    # )
    metadata = event_records[
        0
    ].event_log_entry.dagster_event.event_specific_data.materialization.metadata

    raw_path = metadata.get("path")
    if not raw_path:
        raise ValueError(f"Asset '{asset_key.to_string()}' has no 'path' metadata.")

    if isinstance(raw_path, str):
        file_path = raw_path
    else:
        file_path = raw_path.text  # If stored as TextMetadataValue

    context.log.info(f"Original path: {file_path}")
    file_path = file_path.replace("\\", "/")
    dir_path = os.path.split(file_path)[0]
    wildcard_path = os.path.join(dir_path, "*.parquet").replace("\\", "/")

    context.log.info(f"Wildcard path: {wildcard_path}")

    return wildcard_path


@asset(
    io_manager_key="parquet_io_manager",
    ins={
        "cm": AssetIn(AssetKey(["aggregate_permit_models"])),
    },
    deps=[AssetKey("bps_survey_files")],
    group_name="cm_permits",
    description="join permit data with BPS survey data",
    partitions_def=cm_permit_partitions_def,
    automation_condition=AutomationCondition.eager(),
)
def compose_permit_models(
    context: AssetExecutionContext,
    cm: pd.DataFrame,
) -> Output[pd.DataFrame]:
    """
    Join permit data with BPS survey data.
    """
    cm_file = context.partition_key

    # context.log.info(f"Expected asset key: {AssetKey('bps_survey_files')}")
    # for an_asset in materialized_assets:
    #     context.log.info(f"Materialized Asset: {an_asset.to_string()}"
    asset_key = AssetKey(["bps_survey_files"])
    outpath = get_outpath(context, asset_key)
    # context.log.info(f"outpath: {outpath}")
    # outpath = outpath.replace("{partition_key}", "*")
    # outpath = f"data/{outpath}"

    query = f"""
        SELECT *
        FROM read_parquet('{outpath}')
        WHERE measure = 'Units'
        """
    bps = duckdb.sql(query).to_df()

    bps["Place_Name_upper"] = bps["Place_Name"].str.upper()

    merge_keys = [
        ("jurisdiction", "Place_Name_upper"),
        ("unit_group", "unit_group"),
        ("permit_month", "Survey_Date"),
    ]

    # Add state_fips to merge keys if available in both DataFrames
    if "State_Code" in cm.columns:
        merge_keys.append(("state_fips", "State_Code"))

    # Add county_fips to merge keys if available in both DataFrames
    if "County_Code" in cm.columns:
        merge_keys.append(("county_fips", "County_Code"))

    # Perform the join using the combined condition
    overlap = cm.merge(
        bps,
        how="inner",
        left_on=[key[0] for key in merge_keys],
        right_on=[key[1] for key in merge_keys],
    )
    overlap["cm_file"] = cm_file

    return Output(
        overlap,
        metadata={
            "num_rows": overlap.shape[0],
            "num_columns": overlap.shape[1],
            "preview": overlap.head().to_markdown(),
        },
    )
