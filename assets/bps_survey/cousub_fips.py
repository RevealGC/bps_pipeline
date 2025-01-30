"""scrape census.gov for county fips codes"""

# %%
import pandas as pd
from dagster import (
    Output,
    AssetExecutionContext,
    DynamicPartitionsDefinition,
    asset,
)


from assets.bps_survey.census_helper import get_census_metadata
from utilities.dagster_utils import create_dynamic_partitions

fips_releases_partitions_def = DynamicPartitionsDefinition(name="cousub_fips")


@asset()
def get_metadata(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    """Get metadata for all files on the census.gov FTP server."""
    url = "https://www2.census.gov/geo/docs/reference/codes2020/place_by_cou/"

    df = get_census_metadata(url)

    selected_partitions = df["filename"].unique()

    new_partitions = create_dynamic_partitions(
        context=context,
        dynamic_partiton_def=fips_releases_partitions_def,
        possible_partitions=selected_partitions,
    )
    return Output(
        new_partitions,
        metadata={
            "num_rows": df.shape[0],
            "num_columns": df.shape[1],
            "preview": df.head().to_markdown(),
        },
    )


# %%


def get_county_fips_data():
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
