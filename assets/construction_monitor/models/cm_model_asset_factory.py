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
    def _asset(**inputs) -> dg.Output:
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

