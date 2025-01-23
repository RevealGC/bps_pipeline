# %%
import os
import pandas as pd
from dagster import (
    AssetSelection,
    DynamicPartitionsDefinition,
    MetadataValue,
    Output,
    SensorResult,
    RunRequest,
    asset,
    sensor,
)

# partitoin defintion
cm_permit_partitions_def = DynamicPartitionsDefinition(name="cm_files")


def get_files_metadata(folder_path: str) -> pd.DataFrame:
    """Retrieve filenames and metadata of all files in a specific folder."""
    files_metadata = []

    for root, _, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_info = os.stat(file_path)
            files_metadata.append(
                {
                    "partition": file.replace(".csv", ""),
                    "filename": file,
                    "file_path": file_path,
                    "size": file_info.st_size,
                    "last_modified": pd.to_datetime(file_info.st_mtime, unit="s"),
                }
            )

    return pd.DataFrame(files_metadata)


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
            "num_columns": permit_df.shape[1],
            "preview": MetadataValue.md(permit_df.head().to_markdown()),
            "inferred_schema": MetadataValue.md(permit_df.dtypes.to_markdown()),
        },
    )


@sensor(asset_selection=AssetSelection.assets(cm_permit_files))
def file_sensor(context):
    """Detect new files in specified directories."""
    directories = os.getenv("MY_DIRECTORIES", "").split(os.pathsep)
    new_files = []

    for directory in directories:
        if os.path.exists(directory):  # Ensure the directory exists
            # Check for files that are not already registered as dynamic partitions
            new_files.extend(
                f"{directory}/{filename}"
                for filename in os.listdir(directory)
                if not context.instance.has_dynamic_partition(
                    cm_permit_partitions_def.name, f"{directory}/{filename}"
                )
            )
        else:
            context.log.warning(f"Directory does not exist: {directory}")

    return SensorResult(
        run_requests=[RunRequest(partition_key=filename) for filename in new_files],
        dynamic_partitions_requests=(
            [cm_permit_partitions_def.build_add_request(new_files)] if new_files else []
        ),
    )
