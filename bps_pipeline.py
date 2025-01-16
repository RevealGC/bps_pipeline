"""docstring"""

from dagster import Definitions, load_assets_from_modules
from dagster_duckdb_pandas import DuckDBPandasIOManager

# assets
import assets.bps_survey.bps_survey_data as bps_module
from resources.parquet_io_manager import partitioned_parquet_io_manager

# sensors
# from sensors.release_sensor import release_sensor


defs = Definitions(
    assets=load_assets_from_modules([bps_module]),
    # sensors=[release_sensor],
    resources={
        "parquet_io_manager": partitioned_parquet_io_manager.configured(
            {"base_path": "data/parquet_files"}
        ),
        "ddb_io_manager": DuckDBPandasIOManager(
            database="../data/my_duckdb_database.duckdb",  # required
            schema="bps_pipeline",  # optional, defaults to PUBLIC
        ),
    },
)
