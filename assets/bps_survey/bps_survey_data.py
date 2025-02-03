"""Docstring for the bps_survey_data module."""

from datetime import datetime
from dagster import (
    AssetIn,
    # AssetKey,
    MetadataValue,
    Output,
    DynamicPartitionsDefinition,
    TimeWindowPartitionsDefinition,
    # BackfillPolicy,
    AutomationCondition,
    asset,
)
import pandas as pd

import assets.bps_survey.census_helper as ch
from utilities.dagster_utils import create_dynamic_partitions


# this is duplicated in the cm_raw_data.py file
yearly_partitions_def = TimeWindowPartitionsDefinition(
    start=datetime(2004, 1, 1), cron_schedule="0 0 1 1 *", fmt="%Y"
)

bps_releases_partitions_def = DynamicPartitionsDefinition(name="bps_releases")

shared_params = {
    "io_manager_key": "parquet_io_manager",
    "group_name": "bps_survey",
    "owners": ["elo.lewis@revealgc.com", "team:construction-reengineering"],
    "automation_condition": AutomationCondition.eager(),
}


@asset(
    **shared_params,
    partitions_def=yearly_partitions_def,
    description="List of BPS survey files available on bps FTP server",
)
def bps_survey_releases(context):
    """Get list of BPS survey files from FTP server."""

    partition_key = int(context.partition_key)
    url_base = "https://www2.census.gov/econ/bps/Place/"
    regions = [
        "Midwest",
        "Northeast",
        "South",
        "West",
    ]
    context.log.info(f"the current partiton year : {partition_key}")
    context.log.info(f"retreiving data from {url_base}")
    all_releases = (
        pd.concat(
            [
                ch.get_census_metadata(f"{url_base}{region}%20Region/")
                for region in regions
            ],
            ignore_index=True,
        )
        .assign(
            **pd.DataFrame(
                lambda df: df["filename"].apply(ch.parse_census_filename).tolist()
            )
        )
        .sort_values(by="last_modified")
    )

    context.log.info(f"Found {len(all_releases)} releases.")

    selected_releases_data = all_releases.query(
        "suffix == 'C' and year == @partition_key"
    )

    selected_partitions = (
        selected_releases_data["filename"].str.replace(".txt", "").unique().tolist()
    )

    new_partitions = create_dynamic_partitions(
        context=context,
        dynamic_partiton_def=bps_releases_partitions_def,
        possible_partitions=selected_partitions,
    )
    # all_releases.to_csv("data/bps_survey_releases.csv", index=False, mode="w")
    # return all_releases.sort_values(by="last_modified")
    return Output(
        selected_releases_data,
        metadata={
            "num_files": len(all_releases),
            # "preview": MetadataValue.md(new_partitions.(20).to_markdown()),
            "preview": MetadataValue.md(
                pd.DataFrame(new_partitions).head().to_markdown()
            ),
            # ),
        },
    )


@asset(
    **shared_params,
    partitions_def=bps_releases_partitions_def,
    ins={
        "releases": AssetIn("bps_survey_releases"),
    },
    description="Raw BPS survey files downloaded from FTP",
    # metadata={
    #     "outpath": "bps_raw_survey_files/{partition_key}.parquet",
    # },
)
def bps_survey_files(context, releases: pd.DataFrame) -> Output[pd.DataFrame]:
    """Download and store BPS survey files as parquet."""
    partition_key = context.partition_key
    context.log.info(f"Downloading file: {partition_key}")

    url = releases[releases["filename"] == f"{partition_key}.txt"]["file_url"].values[0]

    raw_bps_survey = pd.read_csv(
        url, encoding="utf-8", index_col=False, skiprows=3, header=None, dtype=str
    )
    total_cols = raw_bps_survey.shape[1]
    id_cols = ch.get_bps_header(url, total_cols)
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

    return Output(
        melted_bps_data,
        metadata={
            "preview": MetadataValue.md(melted_bps_data.head().to_markdown()),
            "rows_downloaded": file_rows,
        },
    )


# # %%
# def enriched_bps_survey_data(
#     context, bps_survey_files: pd.DataFrame
# ) -> Output[pd.DataFrame]:
#     """Aggregate BPS survey data."""
#     df
#     groups = ["1_unit", "2_units", "3_4_units", "5+_units"]

#     df = df[df["unit_group"].isin(groups)]
#     df = (
#         df.groupby(["permit_month", "jurisdiction", "unit_group"])
#         .agg(permit_dwellings=("permit_dwellings", "sum"))
#         .reset_index()
#     )
#     return df
