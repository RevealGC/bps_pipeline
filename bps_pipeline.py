"""docstring"""

from os import getenv
from pathlib import Path
import dagster as dg
from dagster import EnvVar
from dagster_duckdb_pandas import DuckDBPandasIOManager

# assets
from assets.bps_survey import bps_survey_data, cousub_fips
from sensors.cm_files_sensor import file_sensor
from assets.construction_monitor import (
    cm_csv_files,
    cm_transform,
    cm_aggregate,
)


# resources
from resources.parquet_io_manager import partitioned_parquet_io_manager
from resources.cm_ftp_resource import FTPResource

# sensors
# from sensors.release_sensor import release_sensor
BASE_PATH = Path(EnvVar("BASE_PATH").get_value()).expanduser().resolve()

defs = dg.Definitions(
    assets=dg.with_source_code_references(
        dg.load_assets_from_modules(
            [
                bps_survey_data,
                cousub_fips,
                cm_csv_files,
                cm_transform,
                cm_aggregate,
            ]
        )
    ),
    sensors=[file_sensor],
    resources={
        "cm_ftp_resource": FTPResource(
            host=EnvVar("CM_FTP_ADDRESS"),
            username=EnvVar("CM_FTP_USERNAME"),
            password=EnvVar("CM_FTP_PASSWORD"),
        ),
        "parquet_io_manager": partitioned_parquet_io_manager.configured(
            {"base_path": str(BASE_PATH)}
        ),
        "ddb_io_manager": DuckDBPandasIOManager(
            database=str(BASE_PATH.joinpath("my_duckdb_database.duckdb")),  # required
            schema="bps_pipeline",  # optional, defaults to PUBLIC
        ),
    },
)
