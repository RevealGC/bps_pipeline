"""docstring"""

from pathlib import Path
import dagster as dg
from dagster import EnvVar
import warnings

warnings.filterwarnings("ignore", category=dg.ExperimentalWarning)
BASE_PATH = Path(EnvVar("BASE_PATH").get_value()).expanduser().resolve()


# assets
from assets.bps_survey import (
    bps_survey_data,
    cousub_fips,
)
from assets.construction_monitor import (
    cm_csv_files,
    cm_transform,
    cm_aggregate,
    cm_compare,
)
from assets.construction_monitor import cm_files_sensor


# resources
from resources.parquet_io_manager import partitioned_parquet_io_manager
from resources.cm_ftp_resource import FTPResource
from resources.census_mft_resource import MFTResource
from assets.construction_monitor.cm_files_sensor import FileMonitorConfig

# sensors
# from sensors.release_sensor import release_sensor

defs = dg.Definitions(
    assets=dg.with_source_code_references(
        dg.load_assets_from_modules(
            [
                bps_survey_data,
                cousub_fips,
                cm_csv_files,
                cm_transform,
                cm_aggregate,
                cm_compare,
            ]
        )
    ),
    sensors=[
        cm_files_sensor.cm_files_sensor,
        *cousub_fips.census_sensors,
        *bps_survey_data.bps_releases_sensors(),
    ],
    resources={
        "cm_ftp_resource": FTPResource(
            host=EnvVar("CM_FTP_ADDRESS"),
            username=EnvVar("CM_FTP_USERNAME"),
            password=EnvVar("CM_FTP_PASSWORD"),
        ),
        "parquet_io_manager": partitioned_parquet_io_manager.configured(
            {"base_path": str(BASE_PATH)}
        ),
        "census_mft_resource": MFTResource(
            url=EnvVar("MFT_BASE_URL"),
            username=EnvVar("MFT_USERNAME"),
            password=EnvVar("MFT_PASSWORD"),
        ),
        "file_monitor_config": FileMonitorConfig(
            local_directories=[EnvVar("PERMIT_DATA_DIRECTORY")],
            file_pattern=".*.csv",
        ),
    },
)
