# %%
# import os
import pandas as pd
from dagster import MetadataValue, Output, AssetIn, AssetKey, asset, AutomationCondition

from assets.construction_monitor.cm_csv_files import (
    cm_permit_partitions_def,
)


@asset(
    io_manager_key="parquet_io_manager",
    ins={
        "juris": AssetIn(AssetKey(["calculate_jurisdiction"])),
        "months": AssetIn(AssetKey(["calculate_permit_month"])),
        "unitgroups": AssetIn(AssetKey(["calculate_unit_group"])),
        "dwellings": AssetIn(AssetKey(["impute_dwellings"])),
    },
    group_name="cm_permits",
    description="Read raw permit data from a CSV file.",
    partitions_def=cm_permit_partitions_def,
    automation_condition=AutomationCondition.eager(),
)
def aggregate_permit_models(
    context,
    juris: pd.DataFrame,
    months: pd.DataFrame,
    unitgroups: pd.DataFrame,
    dwellings: pd.DataFrame,
) -> Output[pd.DataFrame]:
    """
    load and join model outputs from:
        - calculate_jurisdiction,
        - calculate_permit_month,
        - calculate_unit_group,
        - impute_dwellings
    """
    partition_key = context.partition_key
    juris = juris.set_index(juris.index)
    months = months.set_index(months.index)
    unitgroups = unitgroups.set_index(unitgroups.index)
    dwellings = dwellings.set_index(dwellings.index)

    # if there are no rows in the dataframes, return empty dataframe
    if juris.empty or months.empty or unitgroups.empty or dwellings.empty:
        return Output(None)

    dwellings["permit_dwellings"] = pd.to_numeric(
        dwellings["permit_dwellings"], errors="coerce"
    )
    dwellings["permit_dwellings"] = dwellings["permit_dwellings"].fillna(0)

    # Join all tables by index
    combined = juris.join(months).join(unitgroups).join(dwellings)

    combined["composite_key"] = (
        combined["code_version_months"]
        + "_"
        + combined["code_version_juris"]
        + "_"
        + combined["code_version_unitgroups"]
        + "_"
        + combined["code_version_dwellings"]
    )
    context.log.info(MetadataValue.md(combined.head().to_markdown()))

    # Aggregate: sum D values grouped by A, B, C versions and index
    # grooup by all values except permit_dwellings
    group_by_columns = [col for col in combined.columns if col != "permit_dwellings"]

    aggregated = (
        combined.groupby(group_by_columns)
        .agg({"permit_dwellings": "sum"})
        .reset_index()
    )
    aggregated["partition_key"] = partition_key

    return Output(
        aggregated,
        metadata={
            "num_columns": aggregated.shape[1],
            "num_rows": aggregated.shape[0],
            "preview": MetadataValue.md(aggregated.head().to_markdown()),
        },
    )


# # aggregate_permit_data.py
# reduced_df = permit_data.copy()[["PMT_DATE", "SITE_JURIS", "PMT_UNITS"]]
# calculate_permit_month(reduced_df)
# calculate_jurisdiction(reduced_df)
# calculate_unit_group(reduced_df)
# impute_dwellings(reduced_df)
# print(reduced_df)


# # %%
# def aggregate_permit_data(dataframe):
#     df = dataframe.copy()
#     groups = ["1_unit", "2_units", "3_4_units", "5+_units"]

#     df = df[df["unit_group"].isin(groups)]
#     df = (
#         df.groupby(["permit_month", "jurisdiction", "unit_group"])
#         .agg(permit_dwellings=("permit_dwellings", "sum"))
#         .reset_index()
#     )
#     return df


# aggregate_permit_data(reduced_df)
