# %%
import pandas as pd
import dagster as dg

from assets.construction_monitor.cm_csv_files import (
    cm_permit_files_partitions,
)
from utilities.dagster_utils import create_dynamic_partitions

evaluations_partitions_def = dg.DynamicPartitionsDefinition(name="model_evaluations")


@dg.asset(
    io_manager_key="parquet_io_manager",
    ins={
        "juris": dg.AssetIn(dg.AssetKey(["calculate_jurisdiction"])),
        "months": dg.AssetIn(dg.AssetKey(["calculate_permit_month"])),
        "unitgroups": dg.AssetIn(dg.AssetKey(["calculate_unit_group"])),
        "dwellings": dg.AssetIn(dg.AssetKey(["impute_dwellings"])),
    },
    group_name="cm_permits",
    description="Read raw permit data from a CSV file.",
    partitions_def=cm_permit_files_partitions,
    automation_condition=dg.AutomationCondition.eager(),
)
def aggregate_permit_models(
    context,
    juris: pd.DataFrame,
    months: pd.DataFrame,
    unitgroups: pd.DataFrame,
    dwellings: pd.DataFrame,
) -> dg.Output[pd.DataFrame]:
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
        return dg.Output(pd.DataFrame(), metadata={"num_rows": 0})

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
    context.log.info(dg.MetadataValue.md(combined.head().to_markdown()))

    # Aggregate: sum D values grouped by A, B, C versions and index
    # grooup by all values except permit_dwellings
    group_by_columns = [col for col in combined.columns if col != "permit_dwellings"]

    aggregated = (
        combined.groupby(group_by_columns)
        .agg({"permit_dwellings": "sum"})
        .reset_index()
    )
    aggregated["partition_key"] = partition_key

    create_dynamic_partitions(
        context=context,
        dynamic_partiton_def=evaluations_partitions_def,
        possible_partitions=aggregated["composite_key"].unique().tolist(),
    )

    return dg.Output(
        aggregated,
        metadata={
            "num_columns": aggregated.shape[1],
            "num_rows": aggregated.shape[0],
            "preview": dg.MetadataValue.md(aggregated.head().to_markdown()),
        },
    )
