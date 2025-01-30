"""docstring"""

from typing import List
from datetime import datetime
import pandas as pd
from dagster import (
    asset,
    ResourceParam,
    Output,
    MetadataValue,
    AssetIn,
    BackfillPolicy,
    TimeWindowPartitionsDefinition,
)

from resources.cm_ftp_resource import FTPResource

# this is duplicated in the bps_survey_data.py file
yearly_partitions_def = TimeWindowPartitionsDefinition(
    start=datetime(2004, 1, 1), cron_schedule="0 0 1 1 *", fmt="%Y"
)


@asset(
    group_name="cm_permits",
    description="List of CM permit files available on CM FTP server",
    owners=["elo.lewis@revealgc.com", "team:construction-reengineering"],
)
def cm_file_releases(context, cm_ftp_resource: ResourceParam[FTPResource]) -> List[str]:
    """docstring"""
    all_releases = cm_ftp_resource.fetch_all_filenames()
    context.log.info(f"Found {len(all_releases)} releases.")
    releases_df = pd.DataFrame(all_releases, columns=["filename"])
    releases_df.to_csv("data/cm_permit_releases.csv", index=False)

    return Output(
        all_releases,
        metadata={
            "num_files": len(all_releases),
            "preview": MetadataValue.md(releases_df.tail(20).to_markdown()),
        },
    )


@asset(
    ins={"releases": AssetIn("cm_file_releases")},
    group_name="cm_permits",
    description="Partition mapping for weekly CM permit files",
    partitions_def=yearly_partitions_def,
    backfill_policy=BackfillPolicy.single_run(),
    owners=["elo.lewis@revealgc.com", "team:construction-reengineering"],
)
def update_bps_survey_partitions(context, releases: pd.DataFrame) -> Output[None]:
    """Update dynamic partitions based on new bps releases."""
    releases_data = releases

    # Get the most recent release year
    partition_key = context.partition_key
    context.log.info(f"the current partiton year : {partition_key}")

    selected_releases_data = releases_data[releases_data["year"] == int(partition_key)]

    selected_partitions = (
        selected_releases_data["filename"].str.replace(".txt", "").unique().tolist()
    )
    context.log.info(f"the current year contains : {len(selected_partitions)} files")
    existing_partition_keys = cm_file_releases.get_partition_keys(
        dynamic_partitions_store=context.instance
    )
    context.log.info(f"existing partitions : {len(existing_partition_keys)} files")

    new_partitions = [
        p for p in selected_partitions if p not in existing_partition_keys
    ]

    if len(new_partitions) > 0:
        context.log.info(f"partitions to add: {len(new_partitions)} files")

        for new_partition in new_partitions:
            context.log.info(f"Adding partition for file: {new_partition}")
            context.instance.add_dynamic_partitions(
                cm_file_releases.name, [new_partition]
            )

    return Output(
        None,
        metadata={
            "num_partitions": len(new_partitions),
            "preview": MetadataValue.md(
                pd.DataFrame({"partition_key": new_partitions}).head().to_markdown()
            ),
        },
    )


# add partitions from filenames
# download files as parquet (partitioned by file)

# join_assets and melt
# join and compare bps vs cm

# future
# deduplication by address
# deduplication by permit number
