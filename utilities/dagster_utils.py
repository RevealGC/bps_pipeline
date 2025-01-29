from dagster import DynamicPartitionsDefinition, AssetExecutionContext


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
