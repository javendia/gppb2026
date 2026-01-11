# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

workspace_id = None
dataset_id = None
partitions_config = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
import logging
import sys
import json
from fabtoolkit.utils import (
    validate_json,
    generate_date_ranges,
    Constants,
    Interval
)
from fabtoolkit.log import ConsoleLogFormatter
from fabtoolkit.dataset import Dataset

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Constants
DEFAULT_LOG_LEVEL = logging.DEBUG
DATE_FORMAT = Constants.DATE_FORMAT

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

logger = logging.getLogger("partitioner")
logger.setLevel(DEFAULT_LOG_LEVEL)
logger.propagate = False

if not logger.handlers:
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ConsoleLogFormatter())
    console_handler.setLevel(DEFAULT_LOG_LEVEL)
    logger.addHandler(console_handler)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def _validate_params(dataset: Dataset, partitions_config: str) -> pd.DataFrame:
    """
    Validates partitions configuration parameter.
    
    Args:
        dataset (Dataset): Dataset object to use for validation.
        partitions_config (str): JSON string containing partitions configuration.
        
    Returns:
        pd.DataFrame: Validated configuration dataframe.
        
    Raises:
        ValueError: If partition configuration references invalid tables or columns.
    """

    available_intervals: List[str] = [i.value for i in Interval]

    # Parse and validate configuration JSON
    columns = ["table", "first_date", "partition_by", "interval"]
    config_df: pd.DataFrame = validate_json(partitions_config, columns)

    # Get available tables and columns from dataset
    available_tables: pd.DataFrame = dataset.tables

    # Find mismatches between configuration and actual dataset schema
    invalid_entries: pd.DataFrame = config_df.merge(
        available_tables, 
        left_on=["table", "partition_by"], 
        right_on=["table_name", "column_name"], 
        how="left", 
        indicator=True,
    )
    
    # Identify rows that exist in config but not in dataset
    invalid_entries = invalid_entries[invalid_entries["_merge"] == "left_only"]
    
    if not invalid_entries.empty:
        error_details: List[Dict] = invalid_entries[["table", "partition_by"]].to_dict(orient="records")
        raise ValueError(
            f"Invalid partition configuration found:\n{json.dumps(error_details, indent=2)}"
        ) from None
    
    for first_date, interval in zip(config_df["first_date"], config_df["interval"]):
        try:
            datetime.strptime(str(first_date), Constants.DATE_FORMAT)
        except ValueError as e:
            raise ValueError(f"Invalid date format for first_date: {first_date}") from e
        
        # Check if interval is a valid Interval enum value
        try:
            Interval(interval)
        except ValueError as e:
            raise ValueError(f"Invalid interval: {interval}. Expected: {available_intervals}") from e
    
    return config_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def generate_partition_ranges(
    table: str, 
    first_date: str,
    interval: str
) -> pd.DataFrame:
    """
    Generates partition ranges for a given table based on the specified interval.

    Args:
        table (str): Name of the table for which partitions are being generated.
        first_date (str): The starting date for partitioning.
        interval (str): The interval for partitioning (YEAR, QUARTER, MONTH).

    Returns:
        pd.DataFrame: DataFrame containing the generated partition ranges.

    Raises:
        ValueError: If the interval is invalid or date parsing fails.
    """
    
    # Parse interval and date values
    interval_def = Constants.INTERVALS[Interval(interval.upper())]
    end_interval: str = interval_def.end_interval
    first_date_dt = datetime.strptime(str(first_date), DATE_FORMAT).date()
    end_date = pd.Period(datetime.today(), freq=end_interval).to_timestamp(how="end").date()
    
    # Generate date ranges
    try:
        logger.info(f"Generating dates list between {first_date_dt} and {end_date} with {interval} interval...")
        new_partitions: pd.DataFrame = generate_date_ranges(first_date_dt, end_date, interval).assign(
            table_name=table
        )
        new_partitions["partition_name"] = (
            new_partitions["table_name"] + "_" + 
            pd.to_datetime(new_partitions["range_start"]).dt.strftime(DATE_FORMAT) + "_" + 
            pd.to_datetime(new_partitions["range_end"]).dt.strftime(DATE_FORMAT)
        )
        logger.info(f"Successfully generated {len(new_partitions)} partition(s) for {table}")
    except Exception as e:
        logger.error(f"Error generating date ranges: {str(e)}")
        raise
    
    return new_partitions[["table_name", "partition_name", "range_start", "range_end"]]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def format_query_definition(base_query: str, last_step: str, row: pd.Series) -> str:
    """
    Generates a M-language query definition.

    Args:
        base_query (str): The base query.
        last_step (str): The name of the last step in the base query.
        row (pd.Series): A pandas Series representing a row from the partitions DataFrame.

    Returns:
        str: M-language query definition for the partition.
    """
    
    partition_name: str = row["partition_name"]
    partition_by: str = row["partition_by"]
    start_date = row["range_start"]
    end_date = row["range_end"]
    
    return (
        f"{base_query},\n"
        f"\t{partition_name} = Table.SelectRows({last_step}, each "
        f"[{partition_by}] >= #date({start_date.year},{start_date.month},{start_date.day}) and "
        f"[{partition_by}] <= #date({end_date.year},{end_date.month},{end_date.day}))\n"
        f"in\n"
        f"\t{partition_name}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def partition() -> None:
    """Creates partitions in a Power BI dataset based on the provided configuration."""

    dataset: Dataset = Dataset(workspace_id, dataset_id)
    workspace_name: str = dataset.workspace_name
    dataset_name: str = dataset.dataset_name
    current_partitions: pd.DataFrame = dataset.partitions

    logger.info("Validating partitions configuration parameter value...")
    config_df = _validate_params(dataset, partitions_config)
    
    for row in config_df.itertuples():
        try:
            table = row.table
            first_date = row.first_date
            partition_by = row.partition_by
            interval = row.interval

            logger.info(f"Creating partitions for '{table}' in the '{dataset_name}' dataset within the '{workspace_name}' workspace.")

            new_partitions = generate_partition_ranges(table, first_date, interval).assign(
                partition_by=partition_by
            )

            # Filter partitions of the table being processed
            table_partitions: pd.DataFrame = current_partitions[current_partitions["table_name"]==table]
    
            # Extract base query and last step name to be used for all pending partitions
            logger.info(f"Extracting query definition...")
            base_query, last_step = dataset.extract_query_definition(table_partitions["query"].iloc[0])
            logger.info(f"Query base:\n{base_query}\n")
            
            # Merge and create new partitions if needed
            pending_partitions = new_partitions.merge(
                table_partitions[["table_name", "partition_name"]], 
                left_on=["table_name", "partition_name"], 
                right_on=["table_name", "partition_name"], 
                how="left", 
                indicator=True
            )
            
            pending_partitions: pd.DataFrame = pending_partitions[pending_partitions["_merge"] == "left_only"][
                ["table_name", "partition_by", "partition_name", "range_start", "range_end"]
            ]
        
            if not pending_partitions.empty:
                logger.info(f"Pending partitions: {pending_partitions['partition_name'].tolist()}")
                pending_partitions["query_definition"] = pending_partitions.apply(
                    lambda row: format_query_definition(base_query, last_step, row),
                    axis=1,
                )
                dataset.create_m_partitions(pending_partitions)
                logger.info(f"Created partitions: {pending_partitions['partition_name'].tolist()}")
            else:
                logger.info(f"No pending partitions to create.")
                
            # Delete default partition if present. Its name equals the table name
            if table in table_partitions["partition_name"].values:
                dataset.delete_default_partition(table)
                logger.info(f"Default partition '{table}' deleted successfully.")
            else:
                logger.debug(f"No default partition found.")
        except Exception as e:
            logger.error(f"Failed to create partitions for table '{table}': {str(e)}")
            raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

if __name__ == "__main__":
    partition()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
