"""parquet IO manager for Dagster pipelines."""

import os
import re
from typing import Optional
import pandas as pd
import dagster as dg
from dagster_shared.seven.temp_dir import get_system_temp_directory
from pydantic import Field


def sanitize_filename(filename: str, max_length: int = 255) -> str:
    """
    Sanitize a string to make it safe for use as a filename.

    Args:
        filename (str): The original string to sanitize.
        max_length (int): Maximum allowed length for the filename.

    Returns:
        str: A sanitized version of the filename.
    """
    filename = re.sub(r'[<>:"/\\|?*]', "_", filename)  # Replace invalid characters
    filename = re.sub(r"\s+", "_", filename)  # Replace spaces with underscores
    filename = filename.strip(" ._")  # Remove leading/trailing spaces
    return filename[:max_length]  # Truncate filename to max length


class PartitionedParquetIOManager(dg.ConfigurableIOManager):
    """
    A unified IOManager for handling Parquet files in a Dagster pipeline.

    Supports both local and S3 storage for pandas DataFrames.
    Uses partition keys in file paths for partitioned assets.

    Attributes:
        base_path (str): Local directory for Parquet files (default: system temp dir).
        s3_bucket (str, optional): If provided, stores files in S3 at s3://{s3_bucket}.

    Methods:
        handle_output(context, obj): Saves a DataFrame as a Parquet file.
        load_input(context): Loads a DataFrame from a Parquet file.
    """

    base_path: str = get_system_temp_directory()
    s3_bucket: Optional[str] = None

    @property
    def _base_path(self) -> str:
        """Return the appropriate base path (local or S3)."""
        return f"s3://{self.s3_bucket}" if self.s3_bucket else self.base_path

    def _resolve_path(
        self, context: dg.AssetExecutionContext, mode: str = "output"
    ) -> str:
        """
        Generate a file path based on asset name, partition, and mode.

        Args:
            context (OutputContext | InputContext): Dagster context.
            mode (str): Either 'output' or 'input'.

        Returns:
            str: The resolved file path.
        """
        asset_name = "_".join(context.asset_key.path)

        partition_path = asset_name
        if context.has_partition_key:
            if isinstance(context.partition_key, str):
                partition_path = sanitize_filename(context.partition_key)
            elif isinstance(
                context.partition_key, tuple
            ):  # Handle multi-partition case
                partition_path = os.path.join(
                    *map(sanitize_filename, context.partition_keys)
                )

        upstream_partition_path = "default_upstream"
        if (
            mode == "input"
            and getattr(context, "upstream_output", None) is not None
            and context.upstream_output.has_partition_key
        ):
            up_pk = context.upstream_output.partition_key
            if isinstance(up_pk, str):
                upstream_partition_path = sanitize_filename(up_pk)
            elif isinstance(up_pk, tuple):
                upstream_partition_path = "_".join(map(sanitize_filename, up_pk))

        if mode == "output":
            custom_path = context.metadata.get("inpath") or context.metadata.get(
                "custom_path"
            )
        else:
            if getattr(context, "upstream_output", None) is not None:
                custom_path = context.upstream_output.metadata.get(
                    "inpath"
                ) or context.upstream_output.metadata.get("custom_path")
            else:
                custom_path = None

        # Use custom metadata path if provided
        custom_path = (
            context.metadata.get("custom_path")
            if mode == "output"
            else (
                context.upstream_output.metadata.get("custom_path", None)
                if context.upstream_output is not None
                else None
            )
        )

        if custom_path:
            custom_path = os.path.normpath(custom_path)
            if not os.path.isabs(custom_path):
                return os.path.join(
                    self._base_path,
                    custom_path.format(
                        filename=asset_name,
                        partition_path=partition_path,
                        upstream_partition_key=upstream_partition_path,
                    ),
                )
            return custom_path.format(
                filename=asset_name, partition_path=partition_path
            )

        # Default file path
        return os.path.join(
            self._base_path, sanitize_filename(asset_name), f"{partition_path}.parquet"
        )

    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame):
        """
        Save a pandas DataFrame as a Parquet file.

        Args:
            context (OutputContext): Dagster output context.
            obj (pd.DataFrame): DataFrame to save.
        """
        if isinstance(obj, list):
            context.log.info(
                f"Skipping Parquet save for {context.asset_key.path}, output is a list."
            )
            return  # No need to save a list of filenames as Parquet

        if obj.empty:
            context.log.warning(
                f"Skipping empty DataFrame output for {context.asset_key.path}."
            )
            return

        output_path = self._resolve_path(context, mode="output")
        context.log.info(f"Saving Parquet file to: {output_path}")

        if "://" not in output_path:  # Ensure local directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

        obj.to_parquet(output_path, index=False)

        context.add_output_metadata({"path": output_path, "row_count": len(obj)})
        context.log.info(f"Parquet file saved to: {output_path}")

    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        """
        Load a pandas DataFrame from a Parquet file.

        Args:
            context (InputContext): Dagster input context.

        Returns:
            pd.DataFrame: Loaded DataFrame.
        """
        input_path = self._resolve_path(context, mode="input")
        context.log.info(f"Loading Parquet file from: {input_path}")

        if not os.path.exists(input_path):
            context.log.warning(
                f"File {input_path} not found. Returning empty DataFrame."
            )
            return pd.DataFrame()

        try:
            return pd.read_parquet(input_path)
        except (pd.errors.EmptyDataError, FileNotFoundError, OSError) as e:
            context.log.error(f"Error reading Parquet file {input_path}: {e}")
            return pd.DataFrame()


class S3PartitionedParquetIOManager(PartitionedParquetIOManager):
    """Forces storage in an S3 bucket."""

    s3_bucket: str


class ParquetIOManagerConfig(dg.Config):
    """Pydantic config for the IO manager."""

    base_path: str = Field(default_factory=get_system_temp_directory)
    s3_bucket: Optional[str] = None


@dg.io_manager
def partitioned_parquet_io_manager(init_context) -> PartitionedParquetIOManager:
    """Dagster IO manager factory function."""
    config_data = init_context.resource_config
    return PartitionedParquetIOManager(**config_data)
