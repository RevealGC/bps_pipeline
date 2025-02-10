"""cm_files_sensor.py"""

import re
from typing import List
from pathlib import Path
from datetime import datetime, timezone
import dagster as dg

from assets.construction_monitor.cm_csv_files import cm_permit_files_partitions


# class ConfigFileDirectories:
#     def __init__(self, local_directories: List[str], file_pattern: str):
#         self.local_directories = local_directories
#         self.file_pattern = file_pattern


class FileMonitorConfig(dg.ConfigurableResource):
    """
    A Dagster resource for configuring monitored file directories.

    Attributes:
        local_directories (List[str]): List of local directories to monitor.
        file_pattern (str): Regex pattern for filtering file types.
    """

    local_directories: List[str]
    file_pattern: str

    def get_dir(self):
        return self.local_directories

    def get_pattern(self):
        return self.file_pattern


@dg.sensor(
    target=dg.AssetSelection.assets(dg.AssetKey("cm_permit_files")),
    required_resource_keys={"file_monitor_config"},
    minimum_interval_seconds=30,
    description="Detect new files in specified directories.",
)
def cm_files_sensor(context: dg.SensorEvaluationContext) -> dg.SensorResult:
    """Detect new files in specified directories."""
    config = context.resources.file_monitor_config
    batch_size = 4
    file_regex = re.compile(config.get_pattern(), re.IGNORECASE)
    new_files = []
    skip_counter = 0
    for directory in config.get_dir():
        dir_path = Path(directory).resolve()

        if not dir_path.exists():
            context.log.warning(f"Directory does not exist: {directory}")
            continue

        for file_path in dir_path.iterdir():

            if not file_path.is_file() and file_regex.match(file_path.name):
                skip_counter += 1
                continue

            if not context.instance.has_dynamic_partition(
                cm_permit_files_partitions.name, str(file_path.name)
            ):
                file_dict = {
                    "partition_name": str(file_path.name),
                    "file_path": str(file_path),
                    "last_modified": datetime.fromtimestamp(
                        file_path.stat().st_mtime, tz=timezone.utc
                    ).isoformat(),
                    "file_size": str(file_path.stat().st_size),
                }

                new_files.append(file_dict)

    if skip_counter > 0:
        context.log.info(f"Skipped {skip_counter} files that did not match the filter.")

    if len(new_files) > batch_size:
        new_files = new_files[:batch_size]
        context.log.info(
            f"Found {len(new_files)} new files. Selecting {batch_size} for jobs."
        )

    if not new_files:
        context.log.info("No new files found.")
        return dg.SensorResult(run_requests=[], dynamic_partitions_requests=[])

    filenames = [file["partition_name"] for file in new_files]

    add_request = cm_permit_files_partitions.build_add_request(filenames)
    context.log.info(f"Adding new partitions: {filenames}")
    context.instance.add_dynamic_partitions(cm_permit_files_partitions.name, filenames)

    run_requests = [
        dg.RunRequest(
            partition_key=file["partition_name"],
            run_config={
                "ops": {
                    "cm_permit_files": {
                        "config": {
                            "file_path": file["file_path"],
                            "last_modified": file["last_modified"],
                            "size": file["file_size"],
                        }
                    }
                }
            },
        )
        for file in new_files
    ]
    return dg.SensorResult(
        run_requests=run_requests, dynamic_partitions_requests=[add_request]
    )
