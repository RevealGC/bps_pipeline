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
    # Replace invalid characters with underscores
    filename = re.sub(r'[<>:"/\\|?*]', "_", filename)
    # Replace sequences of whitespace with a single underscore
    filename = re.sub(r"\s+", "_", filename)
    # Remove leading/trailing spaces, dots, or underscores
    filename = filename.strip(" ._")
    # Truncate the filename to the max length
    return filename[:max_length]


class PartitionedParquetIOManager(ConfigurableIOManager):
    """
    A unified IOManager for handling Parquet files in a Dagster pipeline.

    This IOManager supports both local and S3 storage for pandas DataFrames.
    It allows for custom paths specified via metadata and partitions assets
    by including the partition key in the file path.

    Attributes:
        base_path (str): The local directory where Parquet files are stored by default.
        s3_bucket (str, optional): If provided, Parquet files will be stored in S3 at s3://{s3_bucket}.
          Otherwise, files are stored locally.

    Methods:
        handle_output(context, obj): Saves a pandas DataFrame as a Parquet file.
        load_input(context): Loads a pandas DataFrame from a Parquet file.

    Usage Example:
        ```python
        from dagster import io_manager, Definitions, asset
        import pandas as pd
        from dagster._seven.temp_dir import get_system_temp_directory

        @io_manager(config_schema={"base_path": str, "s3_bucket": str})
        def partitioned_parquet_io_manager(init_context):
            return PartitionedParquetIOManager(
                base_path=init_context.resource_config.get("base_path", get_system_temp_directory()),
                s3_bucket=init_context.resource_config.get("s3_bucket"),
            )

        @asset(
            io_manager_key="parquet_io_manager",
            metadata={"custom_path": "s3://my-bucket/{partition_key}_{filename}.parquet"},
        )
        def my_asset(context) -> pd.DataFrame:
            data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
            return data

        defs = Definitions(
            assets=[my_asset],
            resources={
                "parquet_io_manager": partitioned_parquet_io_manager.configured(
                    {
                        "base_path": "data/parquet_files",
                        "s3_bucket": "my-bucket"
                    }
                )
            },
        )
        ```
    """

    base_path: str = get_system_temp_directory()
    s3_bucket: Optional[str] = None

    @property
    def _base_path(self) -> str:
        """
        Return the base path to be used.

        If `s3_bucket` is set, then the path will point to S3
        (e.g., 's3://my-bucket'). Otherwise, it points to `base_path`.
        """
        if self.s3_bucket:
            return f"s3://{self.s3_bucket}"
        return self.base_path

    def _resolve_path(self, context, mode: str = "output") -> str:
        """
        Generate the file path based on the context and mode (input/output).

        Args:
            context (OutputContext | InputContext): Dagster context.
            mode (str): Either 'output' or 'input'.

        Returns:
            str: Resolved file path.
        """
        asset_name = "_".join(
            context.asset_key.path
        )  # Convert asset key to a valid string

        partition_key = sanitize_filename(context.partition_key or "default")

        # custom_metadata = context.metadata.get("custom_metadata", "default_value")

        # Use custom metadata path if provided
        custom_path = (
            context.metadata.get("outpath")
            if mode == "output"
            else context.upstream_output.metadata.get("path")
        )
        if custom_path:
            custom_path = os.path.normpath(custom_path)
            if not os.path.isabs(custom_path):
                return os.path.join(
                    self._base_path,
                    custom_path.format(
                        filename=asset_name,
                        partition_key=partition_key,
                        # custom_metadata=custom_metadata,
                    ),
                )
            return custom_path.format(filename=asset_name, partition_key=partition_key)

        # Default path generation
        return os.path.join(self._base_path, f"{asset_name}/{partition_key}.parquet")

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        """
        Save a pandas DataFrame as a Parquet file.

        Args:
            context (OutputContext): Dagster output context.
            obj (pd.DataFrame): DataFrame to save.
        """
        output_path = self._resolve_path(context, mode="output")
        context.log.info(f"Saving Parquet file to: {output_path}")

        if "://" not in output_path:  # Local path handling
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

        obj.to_parquet(output_path, index=False)

        # Add output metadata
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
            raise FileNotFoundError(f"Parquet file not found at: {input_path}")

        return pd.read_parquet(input_path)


class LocalPartitionedParquetIOManager(PartitionedParquetIOManager):
    """
    Local variant of the PartitionedParquetIOManager.

    Forces the base_path to be a local directory.
    """

    base_path: str = "data/parquet_files"


class S3PartitionedParquetIOManager(PartitionedParquetIOManager):
    """
    S3 variant of the PartitionedParquetIOManager.

    Forces the base path to point to an S3 bucket.
    """

    s3_bucket: str


class ParquetIOManagerConfig(Config):
    """Pydantic config for the IO manager."""

    base_path: str = Field(default_factory=get_system_temp_directory)
    s3_bucket: Optional[str] = None


@io_manager
def partitioned_parquet_io_manager(init_context) -> PartitionedParquetIOManager:
    """Dagster IO manager factory function."""
    # Pull config from the resource config
    config_data = init_context.resource_config  # Dict from user-provided config
    return PartitionedParquetIOManager(**config_data)
