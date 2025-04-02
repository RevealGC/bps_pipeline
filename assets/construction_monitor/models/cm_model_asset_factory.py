from functools import wraps
from typing import Callable
import dagster as dg


def build_cm_model_versions_asset(
    model_field_name: str,
    # upstream_asset:dg.SourceAsset,
    version: str, 
    asset_params: dict, 
    asset_func: Callable,
    # func_params: dict = None,
    
) -> dg.AssetsDefinition:
    """
    Factory function to build a Dagster asset for a model field.

    Args:
        model_field_name (str): The name of the field being modeled.
        version (str): The version identifier for this asset.
        asset_params (dict): Additional asset parameters (e.g., metadata, dependencies).
        asset_ins (dict): The asset inputs (predefined for all models).
        asset_func (Callable): The function that implements the model logic.

    Returns:
        dagster.Asset: A wrapped Dagster asset.
    """
    asset_name = f"cm_models_{model_field_name}_{version}"

    # partitions_def=upstream_asset.partitions_def,  # and we definitely want the same partitioning
    # ins={"upstream": dg.AssetIn(upstream_asset.key)},

    @dg.asset(
        **asset_params,
        name=asset_name,
        code_version=version,
    )
    @wraps(asset_func)  # Preserve function metadata
    def _asset(**inputs):
        output_df = asset_func(**inputs)
        output_df[model_field_name + "_version"] = version

        return dg.Output(
            output_df,
            metadata={
                "num_rows": output_df.shape[0],
                "num_columns": output_df.shape[1],
                "preview": dg.MetadataValue.md(output_df.head().to_markdown()),
            },
        )
    return _asset

def factory_assets(shared_params, field_name, active_versions) -> list:
    """Create versioned assets that require multiple DataFrames as input."""

    model_assets = []
    for version in active_versions:
        code_version_str = version.__name__.replace("_", ".")
        model_assets.append(
            {
                "model_field_name": field_name,
                "version": code_version_str,
                "asset_params": {
                    **shared_params,
                    "description": version.__doc__,
                },
                "asset_func": version,
            }
        )

    return model_assets

def build_all_model_assets(assets_params, shared_params) -> list:
    """
    Take a list of assets_params (dictionaries), run them through factory_assets,
    then build each versioned asset with build_cm_model_versions_asset.
    Returns a flat list of Dagster AssetsDefinition objects.
    """
    assets = []

    for p in assets_params:
        # Suppose each p in assets_params has the keys required for factory_assets:
        # {
        #   "field_name": <str>,
        #   "active_versions": <list of callables / functions>,
        #   ...
        # }

        # 1) Merge shared_params if needed, or pass it in directly
        field_name = p["field_name"]
        active_versions = p["active_versions"]
        # Possibly other fields from p as well...

        # 2) Call factory_assets -> returns a list of dictionaries, each describing an asset
        model_definitions = factory_assets(
            shared_params=shared_params,
            field_name=field_name,
            active_versions=active_versions,
        )
        # model_definitions is a list of dicts, e.g.
        # [
        #   {
        #     "model_field_name": ...,
        #     "version": ...,
        #     "asset_params": ...,
        #     "asset_func": ...
        #   },
        #   ...
        # ]

        # 3) For each dictionary, call build_cm_model_versions_asset(**that_dict)
        for definition in model_definitions:
            asset_def = build_cm_model_versions_asset(**definition)
            assets.append(asset_def)

    return assets
