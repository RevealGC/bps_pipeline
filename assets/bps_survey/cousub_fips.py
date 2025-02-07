"""scrape census.gov for county fips codes"""

import pandas as pd
import dagster as dg

from assets.bps_survey.census_helper import CensusHelper

fips_releases_partitions_def = dg.DynamicPartitionsDefinition(name="cousub_fips")

shared_params = {
    "io_manager_key": "parquet_io_manager",
    "group_name": "census_fips",
    "owners": ["elo.lewis@revealgc.com", "team:construction-reengineering"],
    "automation_condition": dg.AutomationCondition.eager(),
}


# census_data_updater = dg.define_asset_job(
#     "daily_refresh", selection=["customer_data", "sales_report"]
# )
# daily_schedule = dg.ScheduleDefinition(
#     job=census_data_updater,
#     weekday="monday",
#     cron_schedule="0 0 * * *",  # Runs at midnight daily
# )


# @asset(
#     **shared_params,
#     description="Get metadata for all files on the census.gov FTP server.",
# )
@dg.sensor(
    minimum_interval_seconds=3600,
    name="county_fips_update_schedule",
    target=dg.AssetSelection.assets("county_fips_data"),
)
def update_county_fips_metadata(
    context: dg.SensorEvaluationContext,
) -> dg.Output[pd.DataFrame]:
    """
    A sensor that monitors the Census FTP server for new FIPS metadata files.

    - Detects new files by comparing with existing dynamic partitions.
    - Creates new partitions and triggers pipeline runs if new files are found.
    """
    url = "https://www2.census.gov/geo/docs/reference/codes2020/place_by_cou/"
    helper = CensusHelper(url)
    helper.get_url_metadata()
    context.log.info(f"Found {len(helper.files)} files on {url}")

    new_files = []
    for file in helper.files:
        partition_name = file["filename"]

        # Check if partition already exists
        if not context.instance.has_dynamic_partition(
            fips_releases_partitions_def.name, str(partition_name)
        ):
            new_files.append(file)
    context.log.info(f"Found {len(new_files)} new files: {new_files}")

    if not new_files:
        return dg.SensorResult(run_requests=[], dynamic_partitions_requests=[])

    filenames = [file["filename"] for file in new_files]
    add_request = fips_releases_partitions_def.build_add_request(filenames)

    run_requests = [
        dg.RunRequest(
            partition_key=file["filename"],
            run_config={
                "ops": {
                    "county_fips_data": {
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


@dg.asset(
    **shared_params,
    config_schema={"file_url": str, "last_modified": str, "size": str},
    deps=[dg.AssetKey("county_fips_metadata")],
    partitions_def=fips_releases_partitions_def,
)
def county_fips_data(context: dg.AssetExecutionContext) -> dg.Output[pd.DataFrame]:
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
