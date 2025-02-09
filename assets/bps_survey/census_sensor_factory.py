"""
# census_sensor_factory.py

"""

import dagster as dg
from assets.bps_survey.census_helper import CensusHelper


def build_census_sensor(
    target: str,
    census_url: str,
    partition_def: dg.DynamicPartitionsDefinition,
    description: str = None,
) -> dg.SensorDefinition:
    """
    Build a sensor that monitors the Census FTP server for new files.
    """

    @dg.sensor(
        name=f"census_{target}_sensor",
        minimum_interval_seconds=3600,
        asset_selection=dg.AssetSelection.assets(dg.AssetKey(target)),
        description=description,
    )
    def _sensor(context: dg.SensorEvaluationContext):
        helper = CensusHelper(census_url)
        helper.get_url_metadata()
        context.log.info(f"Found {len(helper.files)} files on {census_url}")

        # find new partitions that need to be added
        new_files = []
        for file in helper.files:
            partition_name = file["filename"]

            # Check if partition already exists
            if not context.instance.has_dynamic_partition(
                partition_def.name, str(partition_name)
            ):
                new_files.append(file)

        context.log.info(f"Found {len(new_files)} new files: {new_files}")

        if not new_files:
            context.log.info("No new files found.")
            return dg.SensorResult(run_requests=[], dynamic_partitions_requests=[])

        filenames = [file["filename"] for file in new_files]

        # add partitons for new files
        add_request = partition_def.build_add_request(filenames)
        context.log.info(f"Adding new partitions: {filenames}")
        context.instance.add_dynamic_partitions(partition_def.name, filenames)

        run_requests = [
            dg.RunRequest(
                partition_key=file["filename"],
                run_config={
                    "ops": {
                        target: {
                            "config": {
                                "file_url": file["file_url"],
                                "last_modified": file["last_modified"],
                                "size": file["size"],
                            }
                        }
                    }
                },
            )
            for file in new_files
        ]

        return dg.SensorResult(
            run_requests=run_requests,
            dynamic_partitions_requests=[add_request],
        )

    return _sensor
