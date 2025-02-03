"""docstring"""

from dagster import (
    Definitions,
    EnvVar,
    load_assets_from_modules,
    with_source_code_references,
)
from dagster_duckdb_pandas import DuckDBPandasIOManager

# assets
import assets.bps_survey.bps_survey_data as bps_module
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
BASE_PATH = "data"

defs = Definitions(
    assets=with_source_code_references(
        load_assets_from_modules([bps_module, cm_csv_files, cm_transform, cm_aggregate])
    ),
    sensors=[file_sensor],
    resources={
        "cm_ftp_resource": FTPResource(
            host=EnvVar("CM_FTP_ADDRESS"),
            username=EnvVar("CM_FTP_USERNAME"),
            password=EnvVar("CM_FTP_PASSWORD"),
        ),
        "parquet_io_manager": partitioned_parquet_io_manager.configured(
            {"base_path": BASE_PATH}
        ),
        "ddb_io_manager": DuckDBPandasIOManager(
            database="data/my_duckdb_database.duckdb",  # required
            schema="bps_pipeline",  # optional, defaults to PUBLIC
        ),
    },
)
