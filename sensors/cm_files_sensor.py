"""cm_files_sensor.py"""

import os
from pathlib import Path
import dagster as dg

from assets.construction_monitor.cm_csv_files import cm_permit_files_partitions


@dg.sensor(asset_selection=dg.AssetSelection.assets(dg.AssetKey("cm_permit_files")))
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
                cm_permit_files_partitions.name, str(file_path)
            ):
                new_files.append(str(file_path))

    context.log.info(f"Found {len(new_files)} new files: {new_files}")

    return dg.SensorResult(
        run_requests=[dg.RunRequest(partition_key=filename) for filename in new_files],
        dynamic_partitions_requests=(
            [cm_permit_files_partitions.build_add_request(new_files)]
            if new_files
            else []
        ),
    )
