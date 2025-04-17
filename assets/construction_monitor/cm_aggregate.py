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


# -----------------------------------------------------------------
# new aggregates asset for cm_permits group
# -----------------------------------------------------------------

# get a list of all asset combinations
# for each file, we return one result for each asset combination
# %%
import sys
from itertools import product
import duckdb
import pandas as pd
import dagster as dg
if not "C:/Users/ndece/Github/bps_pipeline/" in sys.path:
    sys.path.append("C:/Users/ndece/Github/bps_pipeline/")

from assets.construction_monitor.cm_transform import assets_params
from assets.construction_monitor.cm_csv_files import cm_permit_files_partitions
from pathlib import Path

model_combinations_partition_def = dg.DynamicPartitionsDefinition(name="model_combinations")

def get_all_versions(assets_params):
    all_versions = {}
    for asset in assets_params:
        if asset["field_name"] in ['permit_month', 'unit_group', 'jurisdiction', 'permit_dwellings']:
            versions = [version.__name__ for version in asset["active_versions"]]
            all_versions[asset["field_name"]] = versions
    return all_versions

def get_all_version_combinations(active_versions_dict):
    """
    Returns a list of partition labels, each describing one combination of
    active versions across all fields.
    Example result item: 'permit_month=v0_0_2|unit_group=v0_0_2|jurisdiction=v0_0_2|permit_dwellings=v0_0_2'
    """
    fields = sorted(active_versions_dict.keys())  # ensure consistent ordering
    # Create a list of lists, each containing the active versions for that field
    versions_list_of_lists = [active_versions_dict[f] for f in fields]

    combos = []
    for combo_tuple in product(*versions_list_of_lists):
        # combo_tuple looks like ("v0_0_2", "v0_0_2", ...)
        # Zip with field names to build a stable string
        parts = []
        for field, version in zip(fields, combo_tuple):
            parts.append(f"{field}={version}")
        combos.append(";".join(parts))
    return combos

@dg.sensor(minimum_interval_seconds=60)
def sync_model_combinations_partitions_sensor(context: dg.SensorEvaluationContext):
    # sync_model_combinations(context)
    all_combos = get_all_version_combinations(get_all_versions(assets_params))  
    instance = context.instance

    existing_partitions = instance.get_dynamic_partitions(model_combinations_partition_def.name)
    existing_set = set(existing_partitions)
    new_set = set(all_combos)


    # Add any new combos
    to_add = new_set - existing_set
    if to_add:
        instance.add_dynamic_partitions(model_combinations_partition_def.name, list(to_add))

    # Remove combos that are no longer valid
    to_remove = existing_set - new_set
    for partition_key in to_remove:
        instance.delete_dynamic_partition(model_combinations_partition_def.name, partition_key)

    # yield dg.RunRequest(run_key=None, run_config={})
    # This will run the job to sync partitions
# %%

@dg.asset(
    io_manager_key="parquet_io_manager",
    partitions_def=dg.MultiPartitionsDefinition(
        {
            "model_combo": model_combinations_partition_def,
            "cm_filename": cm_permit_files_partitions,
        }
    ),
    )
def aggregate_model_combinations(context) -> dg.Output[pd.DataFrame]:
    """
    load and join model outputs from:
        - calculate_jurisdiction,
        - calculate_permit_month,
        - calculate_unit_group,
        - impute_dwellings
    """
    keys_by_dimension: dg.MultiPartitionKey = context.partition_key.keys_by_dimension
    combo = keys_by_dimension["model_combo"]
    filename = keys_by_dimension["cm_filename"]    
    data_path = ".data/"

    filename = "reveal-gc-2024-46.csv"
    
    field_versions = {}
    for part in combo.split(";"):
        field, version = part.split("=")
        field_versions[field] = version
    context.log.info(combo)
    context.log.info(field_versions)

    ctes = []
    from_keys = []
    for key, value in field_versions.items():
        asset_folder_name = Path(f"{data_path}/cm_models_{key}_{value.replace('_', '.')}/{filename}.parquet")
        context.log.info(asset_folder_name)
        cte = f"""{key} AS (
            SELECT *
            FROM read_parquet('{asset_folder_name}', file_row_number = TRUE)
    )"""
        ctes.append(cte)
        from_keys.append(key)

    context.log.info(dg.MetadataValue.md("\n".join(ctes)))
    context.log.info(dg.MetadataValue.md("\n".join(from_keys)))
    cte_with = ",\n".join(ctes)
    sql = f"""
    WITH
        {cte_with}
    SELECT * FROM {from_keys[0]}
    join {from_keys[1]} USING (file_row_number)
    join {from_keys[2]} USING (file_row_number)
    join {from_keys[3]} USING (file_row_number)
    """
    # print(sql)
    result = duckdb.sql(sql).df()
    # print(result)
            # print(Path(asset_folder_name).exists())

    return dg.Output(
        result,
        metadata={
            "num_columns": result.shape[1],
            "num_rows": result.shape[0],
            "preview": dg.MetadataValue.md(result.head().to_markdown()),
        },
    )



