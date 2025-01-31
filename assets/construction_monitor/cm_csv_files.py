"""cm_csv_files.py"""

import pandas as pd
import dagster as dg


cm_permit_partitions_def = dg.DynamicPartitionsDefinition(name="cm_files")


@dg.asset(
    io_manager_key="parquet_io_manager",
    group_name="cm_permits",
    description="Read raw permit data from a CSV file.",
    partitions_def=cm_permit_partitions_def,
)
def cm_permit_files(context) -> dg.Output[pd.DataFrame]:
    """Read raw permit data from a CSV file."""
    pk_file = context.partition_key

    context.log.info(f"Reading permit data from {pk_file}.")
    permit_df = pd.read_csv(
        pk_file, encoding="ISO-8859-1", dtype_backend="pyarrow", dtype=str
    )
    permit_df.fillna("", inplace=True)

    return dg.Output(
        permit_df,
        # None,
        metadata={
            "num_rows": permit_df.shape[0],
            "num_columns": permit_df.shape[1],
            "preview": dg.MetadataValue.md(permit_df.head().to_markdown()),
            "inferred_schema": dg.MetadataValue.md(
                str(permit_df.dtypes.to_frame().to_markdown())
            ),
        },
    )
