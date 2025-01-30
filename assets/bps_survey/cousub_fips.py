"""scrape census.gov for county fips codes"""

# %%
import pandas as pd
from dagster import (
    Output,
    AssetExecutionContext,
    AutomationCondition,
    AssetKey,
    DynamicPartitionsDefinition,
    asset,
)


from assets.bps_survey.census_helper import get_census_metadata
from utilities.dagster_utils import create_dynamic_partitions

fips_releases_partitions_def = DynamicPartitionsDefinition(name="cousub_fips")

shared_params = {
    "io_manager_key": "parquet_io_manager",
    "group_name": "cm_permits",
    "owners": ["elo.lewis@revealgc.com", "team:construction-reengineering"],
    "automation_condition": AutomationCondition.eager(),
}


@asset(shared_params)
def county_fips_metadata(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    """Get metadata for all files on the census.gov FTP server."""
    url = "https://www2.census.gov/geo/docs/reference/codes2020/place_by_cou/"

    df = get_census_metadata(url)

    selected_partitions = df["filename"].unique()

    new_partitions = create_dynamic_partitions(
        context=context,
        dynamic_partiton_def=fips_releases_partitions_def,
        possible_partitions=selected_partitions,
    )
    context.log.info(f"Added {len(new_partitions)} new partitions")
    return Output(
        df,
        metadata={
            "num_rows": df.shape[0],
            "num_columns": df.shape[1],
            "new_partitons": new_partitions.head().to_markdown(),
        },
    )


# %%

# from ...bps_survey.census_helper import get_census_metadata


@asset(
    **shared_params,
    deps=[AssetKey("county_fips_metadata")],
    partitions_def=fips_releases_partitions_def,
)
def get_county_fips_data(context) -> Output[pd.DataFrame]:
    """
    retrieve data for each file. eg.
    url = ftp_address + "st01_al_place_by_county2020.txt"

    STATE|STATEFP|COUNTYFP|COUNTYNAME|PLACEFP|PLACENS|PLACENAME|TYPE|CLASSFP|FUNCSTAT
    AL|01|001|Autauga County|03220|02405187|Autaugaville town|INCORPORATED PLACE|C1|A
    AL|01|001|Autauga County|06460|02405265|Billingsley town|INCORPORATED PLACE|C1|A
    AL|01|001|Autauga County|46600|02582686|Marbury CDP|CENSUS DESIGNATED PLACE|U1|S
    AL|01|001|Autauga County|48712|02404261|Millbrook city|INCORPORATED PLACE|C1|A
    AL|01|001|Autauga County|60264|02582694|Pine Level CDP|CENSUS DESIGNATED PLACE|U1|S
    AL|01|001|Autauga County|62328|02404568|Prattville city|INCORPORATED PLACE|C1|A
    AL|01|003|Baldwin County|04660|02403825|Bay Minette city|INCORPORATED PLACE|C1|A
    AL|01|003|Baldwin County|08272|02633314|Bon Secour CDP|CENSUS DESIGNATED PLACE|U1|S
    """
    ftp_address = "https://www2.census.gov/geo/docs/reference/codes2020/place_by_cou/"
    partition_key = context.partition_key
    # expeting partition_key to be something like "st01_al_place_by_county2020.txt"
    context.log.info(f"Getting data for {partition_key}")
    url = ftp_address + partition_key
    context.log.info(f"attempting to retrieve data from {url}")

    df = pd.read_csv(url, sep="|")

    return Output(df, metadata={"num_rows": df.shape[0], "num_columns": df.shape[1]})
