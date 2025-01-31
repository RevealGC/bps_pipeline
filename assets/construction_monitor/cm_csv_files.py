# %%
import os
import pandas as pd
from pathlib import Path

# from datetime import datetime
from dagster import (
    MetadataValue,
    Output,
    RunRequest,
    DynamicPartitionsDefinition,
    asset,
    sensor,
    SensorResult,
    AssetSelection,
)

# cm_permit_csv_partitions_def = DynamicPartitionsDefinition(name="cm_csv_files")
cm_permit_partitions_def = DynamicPartitionsDefinition(name="cm_files")


@asset(
    io_manager_key="parquet_io_manager",
    group_name="cm_permits",
    description="Read raw permit data from a CSV file.",
    partitions_def=cm_permit_partitions_def,
)
def cm_permit_files(context) -> Output[pd.DataFrame]:
    """Read raw permit data from a CSV file."""
    pk_file = context.partition_key

    context.log.info(f"Reading permit data from {pk_file}.")
    permit_df = pd.read_csv(
        pk_file, encoding="ISO-8859-1", dtype_backend="pyarrow", dtype=str
    )
    permit_df.fillna("", inplace=True)

    return Output(
        permit_df,
        # None,
        metadata={
            "num_rows": permit_df.shape[0],
            "num_columns": permit_df.shape[1],
            "preview": MetadataValue.md(permit_df.head().to_markdown()),
            "inferred_schema": MetadataValue.md(
                str(permit_df.dtypes.to_frame().to_markdown())
            ),
        },
    )


@sensor(asset_selection=AssetSelection.assets(cm_permit_files))
def file_sensor(context):
    """Detect new files in specified directories."""
    directories = os.getenv("MY_DIRECTORIES", "").split(os.pathsep)
    new_files = []

    for directory in directories:
        dir_path = Path(directory)

        if not dir_path.exists():
            context.log.warning(f"Directory does not exist: {directory}")
            continue

        for file_path in dir_path.glob("*.csv"):
            if not context.instance.has_dynamic_partition(
                cm_permit_partitions_def.name, str(file_path)
            ):
                new_files.append(str(file_path))

    context.log.info(f"Found {len(new_files)} new files: {new_files}")

    return SensorResult(
        run_requests=[RunRequest(partition_key=filename) for filename in new_files],
        dynamic_partitions_requests=(
            [cm_permit_partitions_def.build_add_request(new_files)] if new_files else []
        ),
    )
