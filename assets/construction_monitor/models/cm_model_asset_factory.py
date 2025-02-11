from functools import wraps
from typing import Callable
import dagster as dg


def build_cm_model_versions_asset(
    model_field_name: str, version: str, asset_params: dict, asset_func: Callable
) -> dg.Asset:
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
    asset_name = f"cm_{model_field_name}_{version}"

    @dg.asset(
        **asset_params,
        name=asset_name,
        code_version=version,
    )
    @wraps(asset_func)  # Preserve function metadata
    def _asset(context, **kwargs):
        context.log.info(f"Running {asset_name} asset")
        return asset_func(context, **kwargs)

    return _asset
