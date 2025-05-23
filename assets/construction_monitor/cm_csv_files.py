"""cm_csv_files.py"""

import os
import re
import json
from typing import List, Generator
import pandas as pd
import dagster as dg
from utilities.dagster_utils import create_dynamic_partitions
from assets.construction_monitor.cm_helper import rename_cm_weekly_file

cm_ftp_files_partitions = dg.DynamicPartitionsDefinition(name="cm_ftp_files")
cm_imputation_files_partitions = dg.DynamicPartitionsDefinition(name="imputation_files")
cm_issued_date_files_partitions = dg.DynamicPartitionsDefinition(
    name="issued_date_files"
)
cm_raw_files_partitions = dg.DynamicPartitionsDefinition(name="cm_raw_files")
cm_permit_files_partitions = dg.DynamicPartitionsDefinition(name="cm_files")

shared_params = {
    "group_name": "cm_raw_permits",
    # "automation_condition": dg.AutomationCondition.missing(),
    "owners": ["elo.lewis@revealgc.com", "team:construction-reengineering"],
}


@dg.multi_asset(
    outs={
        "cm_ftp_files": dg.AssetOut(
            **shared_params,
            metadata={"description": "raw permit files"},
        ),
        "cm_imputation_files": dg.AssetOut(
            **shared_params,
            metadata={"description": "Permit imputation changes"},
            is_required=False,
        ),
        "cm_issued_date_files": dg.AssetOut(
            **shared_params,
            metadata={"description": "Issued dates files"},
            is_required=False,
        ),
    },
    required_resource_keys={"cm_ftp_resource"},
    description="Fetch files from FTP, categorize them into permits, imputation, and issued-date files, and create dynamic partitions for each type.",
)
def cm_file_releases(
    context,
) -> Generator[dg.Output[List[str]], None, None]:
    """
    Fetch files from FTP, categorize them into core, imputation, and issued-date files,
    and create dynamic partitions for each type.
    """
    cm_ftp_resource = context.resources.cm_ftp_resource
    cm_ftp = cm_ftp_resource.create_client()

    all_files = cm_ftp.list_files()

    context.log.info(f"Found {len(all_files)} releases on ftp.")

    partition_mapping = {
        "cm_ftp_files": (
            re.compile(r"^reveal-gc-\d{4}-\d+\.csv$"),
            cm_ftp_files_partitions,
        ),
        "cm_imputation_files": (
            re.compile(r"^reveal-gc-permit-imputations-\d{4}-\d+\.csv$"),
            cm_imputation_files_partitions,
        ),
        "cm_issued_date_files": (
            re.compile(r"^reveal-gc_issued-dates-\d{4}-\d{2}-\d{2}\.csv$"),
            cm_issued_date_files_partitions,
        ),
    }

    for asset_name in context.selected_output_names:
        if asset_name in partition_mapping:
            pattern, partition_def = partition_mapping[asset_name]
            matching_files = [f for f in all_files if pattern.match(f)]

            pattern, partition_def = partition_mapping[asset_name]

            new_partitions = create_dynamic_partitions(
                context=context,
                dynamic_partiton_def=partition_def,
                possible_partitions=matching_files,
            )

            yield dg.Output(
                new_partitions,
                output_name=asset_name,
                metadata={
                    "num_files": len(matching_files),
                    "preview": dg.MetadataValue.md(
                        pd.DataFrame(matching_files).to_markdown()
                    ),
                },
            )
        else:
            context.log.info(f"Skipping {asset_name}.")


@dg.asset(
    **shared_params,
    required_resource_keys={"cm_ftp_resource"},
    partitions_def=cm_ftp_files_partitions,
    deps={"cm_ftp_files"},
    description="download raw permit files from ftp.",
)
def cm_ftp_csv_files(context) -> dg.Output[None]:
    """
    Download raw permit files from FTP and save locally.

    Returns:
        None: Saves the files to disk.
    """
    directory = dg.EnvVar("PERMIT_DATA_DIRECTORY").get_value()
    partition_key = context.partition_key
    cm_ftp_resource = context.resources.cm_ftp_resource
    cm_ftp = cm_ftp_resource.create_client()

    file_stream = cm_ftp.download_file(partition_key)
    context.log.info(f"Downloaded permit data for {partition_key}.")

    result = pd.read_csv(file_stream, encoding="ISO-8859-1", dtype=str)
    context.log.info(f"Parsed {partition_key} into csv.")

    if result.empty:
        context.log.warning(f"No files found for partition {partition_key}.")
        return dg.Output(None, metadata={"num_records": 0})

    # Save the files to disk if it contains records
    filepath = os.path.join(directory, f"{partition_key}")
    context.log.info(f"Saving permit data to {filepath}.")
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    result.to_csv(filepath, index=False)
    context.log.info(f"Saved permit data to {filepath}")

    return dg.Output(
        None,
        metadata={
            "file_path": filepath,
            "records": len(result),
            "preview": dg.MetadataValue.md(result.head().to_markdown()),
        },
    )


@dg.asset(
    **shared_params,
    io_manager_key="parquet_io_manager",
    config_schema={"file_path": str, "last_modified": str, "size": str},
    required_resource_keys={"census_mft_resource"},
    deps=[dg.AssetKey("cm_ftp_csv_files")],
    partitions_def=cm_raw_files_partitions,
    description="Read raw permit data from a CSV file.",
)
def cm_permit_files(context: dg.AssetExecutionContext) -> dg.Output[pd.DataFrame]:
    """Read raw permit data from a CSV file."""
    config = context.op_config
    context.add_output_metadata(config)

    file_path = config["file_path"]
    mft_resource = context.resources.census_mft_resource
    context.log.info(f"Reading permit data from {file_path}.")

    # try to push file to mft
    mft_client = mft_resource.get_client()
    results = mft_client.send_file(
        target_file=file_path,
        dest_name= f"{context.partition_key}.csv",
        dest_folder="rgc_rawdata_cm",
    )
    results_md = f"```json\n{json.dumps(results, indent=2)}\n```"

    # context.metadata["mft_result"] = results
    # yield dg.Output(results, output_name="mft_cm_permit_files")

    # load file into parquet directory
    permit_df = pd.read_csv(
        file_path, encoding="ISO-8859-1", dtype_backend="pyarrow", dtype=str
    )
    permit_df.fillna("", inplace=True)

    if permit_df.empty:
        context.log.warning(f"No records found in {file_path}.")
        return dg.Output(permit_df, metadata={"num_rows": 0})

    create_dynamic_partitions(
        context=context,
        dynamic_partiton_def=cm_permit_files_partitions,
        possible_partitions=[context.partition_key],
    )

    return dg.Output(
        permit_df,
        # output_name="cm_permit_files",
        metadata={
            "num_rows": permit_df.shape[0],
            "num_columns": permit_df.shape[1],
            "preview": dg.MetadataValue.md(permit_df.head().to_markdown()),
            "inferred_schema": dg.MetadataValue.md(
                str(permit_df.dtypes.to_frame().to_markdown())
            ),
            "source_last_modified": config["last_modified"],
            "source_size": config["size"],
            "source_path": config["file_path"],
            "mft_result": dg.MetadataValue.md(results_md),
        },
    )


# @dg.asset(
#     **shared_params,
#     io_manager_key="parquet_io_manager",
#     deps=[dg.AssetKey("cm_permit_files")],
#     partitions_def=cm_permit_files_partitions,
#     description="Read imputation data from a CSV file.",
# )
# def mft_cm_permit_files(context) -> dg.Output[pd.DataFrame]:
#     """write permit csv to parquet."""
#     partition_key = context.partition_key

#     context.log.info(f"Reading permit data from {partition_key}.")
#     permit_df = pd.read_csv(
#         partition_key, encoding="ISO-8859-1", dtype_backend="pyarrow", dtype=str
#     )
#     permit_df.fillna("", inplace=True)

#     return dg.Output(
#         permit_df,
#         # None,
#         metadata={
#             "num_rows": permit_df.shape[0],
#             "num_columns": permit_df.shape[1],
#             "preview": dg.MetadataValue.md(permit_df.head().to_markdown()),
#             "inferred_schema": dg.MetadataValue.md(
#                 str(permit_df.dtypes.to_frame().to_markdown())
#             ),
#         },
#     )
