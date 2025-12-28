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
tables_to_refresh = ""
partitions_to_refresh = None
commit_mode = "transactional"
max_parallelism = 4

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import pandas as pd
import logging
import sys
from typing import List, Optional
from fabtoolkit.utils import is_valid_text, validate_json, dataframe_to_str
from fabtoolkit.log import ConsoleFormatter
from fabtoolkit.dataset import Dataset

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Constants
DEFAULT_LOG_LEVEL = logging.DEBUG
CHUNK_SIZE = 20

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

logger = logging.getLogger("refresher")
logger.setLevel(DEFAULT_LOG_LEVEL)
logger.propagate = False

if not logger.handlers:
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ConsoleFormatter())
    console_handler.setLevel(DEFAULT_LOG_LEVEL)
    logger.addHandler(console_handler)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def get_tables(dataset: Dataset, tables_to_refresh: Optional[str]) -> pd.DataFrame:
    """
    Gets the list of tables to refresh. 
    If tables parameter is provided, parse it into a list and get related tables. 
    If not provided, retrieve all tables from the dataset.

    Args:
        dataset (Dataset): Dataset object.
        tables_to_refresh (str): Comma-separated string of table names to refresh.

    Returns:
        pd.DataFrame: DataFrame containing table names to refresh.

    Raises:
        ValueError: If no tables to refresh are found or invalid table names are provided.
    """
    
    available_tables: List[str] = dataset.tables["table_name"].unique().tolist()
    
    if is_valid_text(tables_to_refresh):
        table_list: List[str] = [t.strip() for t in tables_to_refresh.split(',') if t.strip()]
        logger.info(f"Tables to refresh provided: {table_list}")
        
        # Check if the provided tables exist in the dataset
        invalid_tables: List[str] = list(set(table_list) - set(available_tables))
        if invalid_tables:
            raise ValueError(f"Invalid table names provided: {invalid_tables}")
        
        # Get related tables
        tables: pd.DataFrame = dataset.get_related_tables(table_list)
        logger.info(f"Tables to refresh: {tables['table_name'].tolist()}")
        return tables
    else:
        logger.info("No tables to refresh provided. Retrieving all tables...")
        return pd.DataFrame({"table_name": available_tables})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def get_partitions(dataset: Dataset, tables: pd.DataFrame, partitions_to_refresh: Optional[str]) -> pd.DataFrame:
    """
    Gets the list of partitions to refresh.
    If partitions parameter is provided, parse it, validate, and filter the partitions to refresh.
    If not provided, retrieve all partitions from the dataset.

    Args:
        dataset (Dataset): Dataset object.
        tables (pd.DataFrame): DataFrame of tables to refresh.
        partitions_to_refresh (str): JSON string specifying tables and their partitions to refresh.
    
    Returns:
        pd.DataFrame: Partitions to refresh.
    
    Raises:
        ValueError: If invalid partitions are specified.
    """

    # Get partitions for each table to refresh
    available_partitions: pd.DataFrame = tables.merge(
        dataset.partitions,
        left_on=["table_name"],
        right_on=["table_name"],
        how="inner"
    )[["table_name", "partition_name"]]

    # If partitions to refresh are specified, validate and filter
    if is_valid_text(partitions_to_refresh):
        selected_partitions: pd.DataFrame = validate_json(partitions_to_refresh, ["table", "selected_partitions"])
        selected_partitions = (
            selected_partitions.explode("selected_partitions", ignore_index=True)
            .rename(columns={"table": "table_name", "selected_partitions": "partition_name"})
        )
        invalid_partitions: pd.DataFrame = selected_partitions.merge(
            available_partitions, 
            left_on=["table_name", "partition_name"],
            right_on=["table_name", "partition_name"],
            how="left",
            indicator=True
        ).query("_merge == 'left_only'").drop(columns=["_merge"])

        # If there are invalid tables or partitions, raise an error
        if not invalid_partitions.empty:
            raise ValueError(
                f"Invalid tables/partitions found:\n{dataframe_to_str(invalid_partitions)}"
            ) from None
        
        # Merge current partitions with selected partitions to determine which to refresh
        table_partitions: pd.DataFrame = available_partitions.merge(
            selected_partitions,
            left_on=["table_name"],
            right_on=["table_name"],
            how="left",
            indicator=True
        )

        # Partitions to be refreshed not explicitly selected (related tables)
        table_partitions_no_selected: pd.DataFrame = (
            table_partitions[table_partitions["_merge"] == "left_only"]
            .rename(columns={"partition_name_x": "partition_name"})[["table_name", "partition_name"]]
        )
        # Partitions to be refreshed explicitly selected
        table_partitions_selected: pd.DataFrame = (
            table_partitions[table_partitions["_merge"] == "both"]
            .rename(columns={"partition_name_y": "partition_name"})[["table_name", "partition_name"]]
            .drop_duplicates()
        )

        partitions: pd.DataFrame = pd.concat(
            [table_partitions_no_selected, table_partitions_selected], 
            ignore_index=True
        )
        logger.info(f"Partitions to refresh: {dataframe_to_str(partitions)}")
        return partitions
    else:
        logger.info("No explicit partitions to refresh. Refreshing all partitions...")
        return available_partitions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def refresh() -> None:
    """
    Refresh specified tables and partitions in a Power BI dataset.
    
    Raises:
        ValueError: If invalid tables or partitions are specified.
        RuntimeError: If the refresh operation fails.
        Exception: If dataset operations fail.
    """
    
    dataset: Dataset = Dataset(workspace_id, dataset_id)

    logger.info(f"Refreshing the '{dataset.dataset_name}' dataset in workspace '{dataset.workspace_name}'...")
    
    try:
        logger.info("Getting tables to refresh...")
        tables: pd.DataFrame = get_tables(dataset, tables_to_refresh)

        logger.info("Getting partitions to refresh...")
        partitions: pd.DataFrame = (
            get_partitions(dataset, tables, partitions_to_refresh)
            .rename(columns={"table_name": "table", "partition_name": "partition"})
        )
    except Exception as e:
        logger.error(f"Failed to retrieve tables and partitions: {str(e)}")
        raise
        
    try:
        logger.info(f"Requesting refresh for objects: {dataframe_to_str(partitions)}")
        
        refresh_request_id: str = dataset.refresh_objects(partitions, commit_mode, max_parallelism)
        if not refresh_request_id:
            logger.error(f"Refresh request is invalid.")
            raise ValueError(f"Refresh request is invalid.")
        
        logger.info(f"Refresh request ID: {refresh_request_id}")
        
        # Check final status
        if dataset.check_refresh_status(refresh_request_id) == "Completed":
            logger.info(f"Refresh completed successfully.")
        else:
            logger.error("Refresh failed. Check refresh history for more details.")
            raise RuntimeError("Refresh failed. Check refresh history for more details.")
    except (ValueError, RuntimeError) as e:
        logger.error(f"Error executing refresh: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during refresh: {str(e)}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

if __name__ == "__main__":
    refresh()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
