import os
from dagster import (
    DynamicPartitionsDefinition,
    AssetExecutionContext,
    EventRecordsFilter,
    DagsterEventType,
    AssetKey,
)


def create_dynamic_partitions(
    context: AssetExecutionContext,
    dynamic_partiton_def: DynamicPartitionsDefinition,
    possible_partitions: list,
):
    """Update dynamic partitions based on based on a provided list."""

    existing_partition_keys = dynamic_partiton_def.get_partition_keys(
        dynamic_partitions_store=context.instance
    )
    context.log.info(f"existing partitions : {len(existing_partition_keys)} files")

    context.log.info(f"the current year contains : {len(possible_partitions)} files")

    new_partitions = [
        p for p in possible_partitions if p not in existing_partition_keys
    ]

    if len(new_partitions) > 0:
        context.log.info(f"partitions to add: {len(new_partitions)} files")

        for new_partition in new_partitions:
            context.log.info(f"Adding partition for file: {new_partition}")
            context.instance.add_dynamic_partitions(
                dynamic_partiton_def.name, [new_partition]
            )
    return new_partitions


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
