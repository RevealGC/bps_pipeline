"""parquet IO manager for Dagster pipelines."""

import os
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
    s3_bucket: str = None

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

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        """
        Save a pandas DataFrame as a Parquet file.

        If a custom output path is provided in the metadata (via "outpath" or any chosen metadata key),
        it will be used. Otherwise, a default path is constructed as:
        {base_path or s3_bucket}/{partition_key or default}_{asset_name}.parquet

        Args:
            context (OutputContext): Dagster output context, including asset_key and partition_key.
            obj (pd.DataFrame): The DataFrame to save as Parquet.
        """

        # Determine the output path
        custom_path = context.metadata.get("outpath") or context.metadata.get(
            "custom_path"
        )
        if custom_path:
            output_path = custom_path.format(
                filename=context.asset_key.path[-1],
                partition_key=context.partition_key or "default",
            )
        else:
            asset_name = context.asset_key.to_string()
            partition_key = context.partition_key or "default"
            output_path = os.path.join(
                self._base_path, f"{partition_key}_{asset_name}.parquet"
            )

        # Create directories for local storage
        # If the path is s3://..., we don't create directories locally
        if "://" not in output_path:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Save the DataFrame to Parquet
        row_count = len(obj)
        obj.to_parquet(output_path, index=False)
        context.add_output_metadata({"row_count": row_count, "path": output_path})
        context.log.info(f"Saved Parquet file to: {output_path}")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """
        Load a pandas DataFrame from a Parquet file.

        If a custom input path is provided in the metadata (via "inpath" or any chosen metadata key),
        it will be used. Otherwise, a default path is constructed as:
        {base_path or s3_bucket}/{partition_key or default}_{asset_name}.parquet

        Args:
            context (InputContext): Dagster input context, including asset_key and partition_key.

        Returns:
            pd.DataFrame: The loaded DataFrame.
        """
        custom_path = context.metadata.get("inpath") or context.metadata.get(
            "custom_path"
        )
        if custom_path:
            input_path = custom_path.format(
                filename=context.asset_key.path[-1],
                partition_key=context.partition_key or "default",
            )
        else:
            asset_name = context.asset_key.to_string()
            partition_key = context.partition_key or "default"
            input_path = os.path.join(
                self._base_path, f"{asset_name}/{partition_key}.parquet"
            )

        # Ensure the file exists if it's a local path
        if "://" not in input_path and not os.path.exists(input_path):
            raise FileNotFoundError(f"Parquet file not found at: {input_path}")

        context.log.info(f"Loading Parquet file from: {input_path}")
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
