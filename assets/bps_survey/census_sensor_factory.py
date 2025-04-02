"""
# census_sensor_factory.py

"""

import re
from typing import Optional
import dagster as dg
from assets.bps_survey.census_helper import CensusHelper


def build_census_sensor(
    target: str,
    census_url: str,
    partition_def: dg.DynamicPartitionsDefinition,
    description: str = None,
    name_prefix: Optional[str] = "",
    name_suffix: Optional[str] = "",
    min_interval_seconds: Optional[int] = 30,
    file_filter: Optional[str] = None,
    batch_size: Optional[int] = 4,
) -> dg.SensorDefinition:
    """
    Build a sensor that monitors the Census FTP server for new files.
    Optionally filters filenames using a regex pattern.

    Args:
        target (str): The asset target name.
        census_url (str): The Census FTP URL to monitor.
        partition_def (DynamicPartitionsDefinition): The dynamic partition definition.
        description (str, optional): Description of the sensor.
        name_prefix (str, optional): Prefix for sensor name.
        name_suffix (str, optional): Suffix for sensor name.
        min_interval_seconds (int, optional): Minimum interval between sensor checks.
        file_filter (str, optional): Regex pattern to filter filenames (default: None).
    """
    name = name_prefix + target + name_suffix

    @dg.sensor(
        name=f"census_{name}_sensor",
        minimum_interval_seconds=min_interval_seconds,
        asset_selection=dg.AssetSelection.assets(dg.AssetKey(target)),
        description=description,
    )
    def _sensor(context: dg.SensorEvaluationContext):
        helper = CensusHelper(census_url)
        helper.get_url_metadata()
        context.log.info(f"Found {len(helper.files)} files on {census_url}")

        # find new partitions that need to be added
        new_files = []
        skip_counter = 0
        for file in helper.files:
            partition_name = file["filename"]

            # Apply regex filter if provided
            if file_filter and not re.match(file_filter, partition_name):
                skip_counter += 1
                continue  # Skip files that don't match the regex

            # Check if partition already exists
            if not context.instance.has_dynamic_partition(
                partition_def.name, str(partition_name)
            ):
                new_files.append(file)
        if skip_counter > 0:
            context.log.info(
                f"Skipped {skip_counter} files that did not match the filter."
            )
        # select 10 files
        if len(new_files) > batch_size:
            new_files = new_files[:batch_size]
            context.log.info(
                f"Found {len(new_files)} new files. Selecting {batch_size} for jobs: {new_files}"
            )

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
                                "url": file["file_url"],
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
