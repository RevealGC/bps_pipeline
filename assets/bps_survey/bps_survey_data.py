"""
b
"""

import dagster as dg
import pandas as pd
from assets.bps_survey.census_helper import get_bps_header
from assets.bps_survey.census_sensor_factory import build_census_sensor

# ---------------------
# Partitions Definitions
# ---------------------

bps_releases_partitions_def = dg.DynamicPartitionsDefinition(name="bps_releases")


# ---------------------
# census_sensors
# ---------------------
def bps_releases_sensors() -> list[dg.SensorDefinition]:
    """Create sensors to monitor Census FTP server for new BPS metadata files."""
    regions = [
        "Midwest",
        "Northeast",
        "South",
        "West",
    ]
    url_base = "https://www2.census.gov/econ/bps/Place/"
    bps_release_sensors = []
    for region in regions:
        bps_release_sensors.append(
            build_census_sensor(
                target="bps_survey_files",
                census_url=f"{url_base}{region}%20Region/",
                partition_def=bps_releases_partitions_def,
                description="Monitor the Census FTP server for new BPS metadata files.",
                name_suffix=f"_{region}",
                file_filter=r".*c\.txt$",  # so9508c.txt
            )
        )
    return bps_release_sensors


# ---------------------
# assets
# ---------------------
@dg.asset(
    io_manager_key="parquet_io_manager",
    group_name="census_publications",
    owners=["elo.lewis@revealgc.com", "team:construction-reengineering"],
    automation_condition=dg.AutomationCondition.eager(),
    config_schema={"url": str, "last_modified": str, "size": str},
    partitions_def=bps_releases_partitions_def,
    description="Raw BPS survey files downloaded from FTP",
)
def bps_survey_files(context) -> dg.Output[pd.DataFrame]:
    """Download and store BPS survey files as parquet."""
    url = context.op_config["url"]
    partition_key = context.partition_key

    context.log.info(f"Downloading file: {partition_key}")

    raw_bps_survey = pd.read_csv(
        url, encoding="utf-8", index_col=False, skiprows=3, header=None, dtype=str
    )

    total_cols = raw_bps_survey.shape[1]
    id_cols = get_bps_header(url, total_cols)
    new_col_names = id_cols.copy()

    # add the last 12 columns to id columns
    groups = ["1_unit", "2_units", "3_4_units", "5+_units"]
    measures = ["Bldgs", "Units", "Value"]
    last_12_names = [f"{group}|{measure}" for group in groups for measure in measures]
    new_col_names.extend(last_12_names)

    raw_bps_survey.columns = new_col_names

    file_rows = len(raw_bps_survey)
    context.log.info(f"file contains records: {file_rows}")

    melted_bps_data = raw_bps_survey.melt(
        id_vars=id_cols,
        value_vars=last_12_names,
    )
    context.log.info(f"file melted: {file_rows}")

    melted_bps_data[["unit_group", "measure"]] = melted_bps_data["variable"].str.split(
        "|", expand=True
    )

    melted_bps_data.drop(
        columns="variable",
        inplace=True,
    )

    return dg.Output(
        melted_bps_data,
        metadata={
            "preview": dg.MetadataValue.md(melted_bps_data.head().to_markdown()),
            "rows_downloaded": file_rows,
            "url": url,
            "last_modified": context.op_config["last_modified"],
            "size": context.op_config["size"],
        },
    )
