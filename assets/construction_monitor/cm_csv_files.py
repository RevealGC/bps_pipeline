"""cm_csv_files.py"""

import re
import pandas as pd
import dagster as dg
from typing import List
from utilities.dagster_utils import create_dynamic_partitions

cm_permit_files_partitions = dg.DynamicPartitionsDefinition(name="cm_files")
cm_imputation_files_partitions = dg.DynamicPartitionsDefinition(name="imputation_files")
cm_issued_date_files_partitions = dg.DynamicPartitionsDefinition(
    name="issued_date_files"
)

shared_params = {
    "io_manager_key": "parquet_io_manager",
    "group_name": "cm_permits",
    "automation_condition": dg.AutomationCondition.eager(),
    "owners": ["elo.lewis@revealgc.com", "team:construction-reengineering"],
}


@dg.multi_asset(
    **shared_params,
    outs={
        "cm_permit_files": dg.AssetOut(
            partitions_def=cm_permit_files_partitions,
            metadata={"description": "raw permit files"},
        ),
        "cm_imputation_files": dg.AssetOut(
            partitions_def=cm_imputation_files_partitions,
            metadata={"description": "Permit imputation changes"},
            is_required=False,
        ),
        "cm_issued_date_files": dg.AssetOut(
            partitions_def=cm_issued_date_files_partitions,
            metadata={"description": "Issued dates files"},
            is_required=False,
        ),
    },
    required_resource_keys={"cm_ftp_resource"},
)
def cm_file_releases(
    context,
) -> dg.Generator[dg.Output[List[str]]]:
    """
    Fetch files from FTP, categorize them into core, imputation, and issued-date files,
    and create dynamic partitions for each type.
    """
    cm_ftp = context.resources.cm_ftp_resource
    all_files = cm_ftp.list_files()

    context.log.info(f"Found {len(all_files)} releases on ftp.")

    partition_mapping = {
        "cm_permit_files": (
            re.compile(r"^reveal-gc-\d{4}-\d+\.csv$"),
            cm_permit_files_partitions,
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

    for asset_name in context.selected_output_names():
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
                metadata={
                    "num_files": len(matching_files),
                    "preview": dg.MetadataValue.md(
                        pd.DataFrame(matching_files).to_markdown()
                    ),
                },
            )
        else:
            context.log.info(f"Skipping {asset_name}.")


# @dg.asset()
# def cm_ftp_files(context) -> dg.Resource:
#     return context.resources.cm_ftp_resource


@dg.asset(
    **shared_params,
    partitions_def=cm_permit_files_partitions,
    description="Read raw permit data from a CSV file.",
)
def cm_permit_files(context) -> dg.Output[pd.DataFrame]:
    """Read raw permit data from a CSV file."""
    partition_key = context.partition_key

    context.log.info(f"Reading permit data from {partition_key}.")
    permit_df = pd.read_csv(
        partition_key, encoding="ISO-8859-1", dtype_backend="pyarrow", dtype=str
    )
    permit_df.fillna("", inplace=True)

    return dg.Output(
        permit_df,
        # None,
        metadata={
            "num_rows": permit_df.shape[0],
            "num_columns": permit_df.shape[1],
            "preview": dg.MetadataValue.md(permit_df.head().to_markdown()),
            "inferred_schema": dg.MetadataValue.md(
                str(permit_df.dtypes.to_frame().to_markdown())
            ),
        },
    )
