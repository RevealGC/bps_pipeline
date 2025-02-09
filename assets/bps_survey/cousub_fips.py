"""scrape census.gov for county fips codes"""

import pandas as pd
import dagster as dg

from assets.bps_survey.census_sensor_factory import build_census_sensor


fips_releases_partitions_def = dg.DynamicPartitionsDefinition(name="cousub_fips")

shared_params = {
    "io_manager_key": "parquet_io_manager",
    "group_name": "census_fips",
    "owners": ["elo.lewis@revealgc.com", "team:construction-reengineering"],
    "automation_condition": dg.AutomationCondition.eager(),
}


census_sensors = [
    build_census_sensor(
        target="county_fips",
        census_url="https://www2.census.gov/geo/docs/reference/codes2020/place_by_cou/",
        partition_def=fips_releases_partitions_def,
        description="Monitor the Census FTP server for new FIPS metadata files.",
    )
]


@dg.asset(
    **shared_params,
    config_schema={"file_url": str, "last_modified": str, "size": str},
    # deps=[dg.AssetKey("county_fips_metadata")],
    partitions_def=fips_releases_partitions_def,
)
def county_fips(context: dg.AssetExecutionContext) -> dg.Output[pd.DataFrame]:
    """
    retrieve data for each fips.
    """
    file_url = context.op_config["file_url"]

    # expeting partition_key to be something like "st01_al_place_by_county2020.txt"
    context.log.info(f"Getting data for partition: {context.partition_key}")
    context.log.info(f"attempting to retrieve data from {file_url}")
    df = pd.read_csv(file_url, sep="|")

    return dg.Output(
        df,
        metadata={
            "num_rows": df.shape[0],
            "num_columns": df.shape[1],
            "url": file_url,
            "last_modified": context.op_config["last_modified"],
            "size": context.op_config["size"],
        },
    )
