"""parquet IO manager for Dagster pipelines."""

import os
import re
import pandas as pd
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    Config,
    io_manager,
)
from dagster._seven.temp_dir import get_system_temp_directory
from pydantic import Field
from typing import Optional


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


class PartitionedParquetIOManager(ConfigurableIOManager):
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

    def _resolve_path(self, context, mode: str = "output") -> str:
        """
        Generate a file path based on asset name, partition, and mode.

        Args:
            context (OutputContext | InputContext): Dagster context.
            mode (str): Either 'output' or 'input'.

        Returns:
            str: The resolved file path.
        """
        asset_name = "_".join(
            context.asset_key.path
        )  # Convert asset key to a valid filename
        partition_key = sanitize_filename(context.partition_key or "")

        # Use custom metadata path if provided
        custom_path = (
            context.metadata.get("custom_path")
            if mode == "output"
            else (
                context.upstream_output.metadata.get("custom_path", None)
                if context.has_upstream_output
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
                        partition_key=partition_key,
                    ),
                )
            return custom_path.format(filename=asset_name, partition_key=partition_key)

        # Default file path
        return os.path.join(
            self._base_path, sanitize_filename(asset_name), f"{partition_key}.parquet"
        )

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
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

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """
        Load a pandas DataFrame from a Parquet file.

        Args:
            context (InputContext): Dagster input context.

        Returns:
            pd.DataFrame: Loaded DataFrame.
        """
        input_path = self._resolve_path(context, mode="input")
        context.log.info(f"Loading Parquet file from: {input_path}")

        if "://" not in input_path and not os.path.exists(input_path):
            raise FileNotFoundError(f"Parquet file not found: {input_path}")

        return pd.read_parquet(input_path)


class S3PartitionedParquetIOManager(PartitionedParquetIOManager):
    """Forces storage in an S3 bucket."""

    s3_bucket: str


class ParquetIOManagerConfig(Config):
    """Pydantic config for the IO manager."""

    base_path: str = Field(default_factory=get_system_temp_directory)
    s3_bucket: Optional[str] = None


@io_manager
def partitioned_parquet_io_manager(init_context) -> PartitionedParquetIOManager:
    """Dagster IO manager factory function."""
    config_data = init_context.resource_config
    return PartitionedParquetIOManager(**config_data)
