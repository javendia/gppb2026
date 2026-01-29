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

workspace_id: str = ""
dataset_id: str  = ""
enable_partition: bool = True
partitions_config: str = ""
enable_refresh: bool = True
tables_to_refresh: str = ""
partitions_to_refresh: str = ""
refresh_commit_mode: str = "transactional"
refresh_max_parallelism: int = 4
notebook_timeout: int = 7200

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import pandas as pd
from datetime import datetime
from typing import Optional, Any, Dict
import logging
import sys
import notebookutils
from io import StringIO
import uuid

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Constants
DEFAULT_LOG_LEVEL = logging.DEBUG
FABTTOOLKIT_VERSION = "1.0.0"
PARTITIONER_NOTEBOOK_NAME = "NB_PAR_PARTITIONER"
REFRESHER_NOTEBOOK_NAME = "NB_PAR_REFRESHER"
AVAILABLE_COMMIT_MODES = {"transactional", "partialBatch"}
DEFAULT_REFRESH_COMMIT_MODE = "transactional"
DEFAULT_REFRESH_MAX_PARALLELISM = 4
DEFAULT_NOTEBOOK_TIMEOUT = 7200

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

wheel_path = f"builtin/fabtoolkit-{FABTTOOLKIT_VERSION}-py3-none-any.whl"
%pip install {wheel_path}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

from fabtoolkit.utils import (
    get_bounds_from_offset,
    generate_date_ranges,
    is_valid_text,
    validate_json,
    Constants
)
from fabtoolkit.log import ConsoleLogFormatter

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

logger = logging.getLogger("custom_refresh_orchestrator")
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

DATE_FORMAT = Constants.DATE_FORMAT

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def validate_params(
        workspace_id: str,
        dataset_id: str,
        enable_partition: bool,
        partitions_config: str,
        enable_refresh: bool,
        tables_to_refresh: Optional[str],
        partitions_to_refresh: Optional[str],
        refresh_commit_mode: Optional[str],
        refresh_max_parallelism: Optional[int],
        notebook_timeout: Optional[int]
) -> Dict[str, Any]:
    """
    Validate input parameters.

    Args:
        workspace_id (str): The workspace identifier.
        dataset_id (str): The dataset identifier.
        enable_partition (bool): Flag to enable partitioning.
        partitions_config (str): JSON string for partitions configuration.
        enable_refresh (bool): Flag to enable refresh.
        tables_to_refresh (Optional[str]): Comma-separated table names to refresh.
        partitions_to_refresh (Optional[str]): JSON string with explicitly defined partitions to refresh.
        refresh_commit_mode (Optional[str]): Commit mode used for the refresh operation.
        refresh_max_parallelism (Optional[int]): Maximum parallelism used for the refresh operation.
        notebook_timeout (Optional[int]): Timeout for the notebook execution.

    Returns:
        Dict[str, Any]: Dictionary containing validated parameters.
    """

    try:
        uuid.UUID(workspace_id)
    except Exception as e:
        logger.error(f"Invalid workspace_id parameter: {str(e)}")
        raise ValueError("Invalid workspace_id parameter.") from e
    try:
        uuid.UUID(dataset_id)
    except Exception as e:
        logger.error(f"Invalid dataset_id parameter: {str(e)}")
        raise ValueError("Invalid dataset_id parameter.") from e

    if not isinstance(enable_partition, bool):
        logger.error("Invalid enable_partition parameter.")
        raise ValueError("Invalid enable_partition parameter.")
    if not isinstance(enable_refresh, bool):
        logger.error("Invalid enable_refresh parameter.")
        raise ValueError("Invalid enable_refresh parameter.")
    
    # Validate partitions_config JSON
    if (enable_partition or enable_refresh) and is_valid_text(partitions_config):
        columns = ["table", "first_date", "partition_by", "interval", "refresh_from", "number_of_intervals"]
        validate_json(partitions_config, columns)
        
    if enable_refresh:
        # Validate tables_to_refresh
        if is_valid_text(tables_to_refresh):
            table_list = [table.strip() for table in tables_to_refresh.split(",")]
            if not all(table for table in table_list):
                logger.error("Invalid tables_to_refresh parameter.")
                raise ValueError("Invalid tables_to_refresh parameter.")

        # Validate partitions_to_refresh JSON
        if is_valid_text(partitions_to_refresh):
            columns = ["table", "selected_partitions"]
            validate_json(partitions_to_refresh, columns)

    # Validate commit mode
    if is_valid_text(refresh_commit_mode):
        if refresh_commit_mode not in AVAILABLE_COMMIT_MODES:
            logger.error(f"Invalid refresh_commit_mode parameter. Available modes: {AVAILABLE_COMMIT_MODES}")
            raise ValueError(f"Invalid refresh_commit_mode parameter. Available modes: {AVAILABLE_COMMIT_MODES}")
    else:
        refresh_commit_mode = DEFAULT_REFRESH_COMMIT_MODE
    
    # Validate max_parallelism
    if refresh_max_parallelism is None:
        refresh_max_parallelism = DEFAULT_REFRESH_MAX_PARALLELISM
    elif not isinstance(refresh_max_parallelism, int) or refresh_max_parallelism <= 0:
        logger.error("Invalid refresh_max_parallelism parameter.")
        raise ValueError("Invalid refresh_max_parallelism parameter.")
    
    # Validate notebook_timeout
    if notebook_timeout is None:
        notebook_timeout = DEFAULT_NOTEBOOK_TIMEOUT
    elif not isinstance(notebook_timeout, int) or notebook_timeout <= 0:
        logger.error("Invalid notebook_timeout parameter.")
        raise ValueError("Invalid notebook_timeout parameter.")
    
    return {
        "workspace_id": workspace_id,
        "dataset_id": dataset_id,
        "enable_partition": enable_partition,
        "partitions_config": partitions_config,
        "enable_refresh": enable_refresh,
        "tables_to_refresh": tables_to_refresh,
        "partitions_to_refresh": partitions_to_refresh,
        "refresh_commit_mode": refresh_commit_mode,
        "refresh_max_parallelism": refresh_max_parallelism,
        "notebook_timeout": notebook_timeout
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def generate_partitions_list(partitions_config: pd.DataFrame) -> str:
    """
    Generates a JSON-formatted string representing partition ranges for each table in the input DataFrame.

    Args:
        partitions_config (pd.DataFrame): DataFrame containing the partitions configuration.

    Returns:
        str: JSON-formatted string with partitions separated by commas for each table.

    Raises:
        Exception: If any step in the process fails.
    """
    
    date_format = Constants.DATE_FORMAT
    date_ranges = []
    
    for row in partitions_config.itertuples():

        logger.info("Calculating bounds for each table...")
        
        try:
            first_date: datetime = datetime.strptime(str(row.first_date), date_format)

            # Determine refresh_from date
            if row.refresh_from == "TODAY":
                refresh_from: datetime = datetime.today()
            else:
                refresh_from: datetime = datetime.strptime(str(row.refresh_from), date_format)

            start_date, end_date = get_bounds_from_offset(
                first_date,
                refresh_from,
                row.interval,
                row.number_of_intervals
            )
            logger.info("Generating date ranges for each table...")
            
            # Generates a list of date ranges with the interval used to create table partitions.
            date_range = generate_date_ranges(start_date, end_date, row.interval).assign(table=row.table)
            date_ranges.append(date_range)
        except Exception as e:
            logger.error(f"Unable to calculate bounds for partitions: {str(e)}")
            raise

    partitions = pd.concat(date_ranges, ignore_index=True)
    logger.info("Composing partition names to refresh...")
    
    try:
        # Generating partitions list like Table_yyyyMMdd_yyyyMM_dd
        partitions["range_start"] = pd.to_datetime(partitions["range_start"]).dt.strftime(date_format)
        partitions["range_end"] = pd.to_datetime(partitions["range_end"]).dt.strftime(date_format)
        partitions["partition"] = partitions.apply(
            lambda row: f"{row['table']}_{row['range_start']}_{row['range_end']}", axis=1
        )
        
        partitions_agg: pd.DataFrame = (
            partitions.groupby("table", as_index=False)
            .agg(selected_partitions=("partition", lambda x: ",".join(x)))
        )
        
        objects = partitions_agg.to_json(orient="records")
        logger.info("List of partitions to refresh created successfully.")
        return objects
    except Exception as e:
        logger.error(f"Unable to format or aggregate partition data: {str(e)}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def run_notebook(notebook_name: str, timeout: int, params: Dict[str, Any]) -> None:
    """
    Runs a Fabric notebook with specified parameters and timeout.

    Args:
        notebook_name (str): Name of the notebook to run.
        timeout (int): Timeout for the refresh execution.
        params (Dict[str, Any]): Dictionary of parameters to pass to the notebook.

    Returns:
        None

    Raises:
        RuntimeError: If notebook execution fails.
    """
    try:
        logger.info(f"Running notebook '{notebook_name}'...")
        notebookutils.notebook.run(notebook_name, timeout, params)
        logger.info(f"Notebook '{notebook_name}' completed successfully.")
    except Exception as e:
        logger.error(f"Failed to execute '{notebook_name}' notebook: {str(e)}")
        raise RuntimeError(f"Notebook execution failed for {notebook_name}: {str(e)}") from e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def run():
    """
    Orchestrates dataset partitioning and refreshing in Power BI.
    
    Raises:
        RuntimeError: If any notebook execution fails
    """

    # Validate input parameters
    params = validate_params(
        workspace_id,
        dataset_id,
        enable_partition,
        partitions_config,
        enable_refresh,
        tables_to_refresh,
        partitions_to_refresh,
        refresh_commit_mode,
        refresh_max_parallelism,
        notebook_timeout
    )
    
    # Create partitions if enable_partition flag is enabled
    if params["enable_partition"]:
        
        logger.info("Partition dataset is enabled.")

        if not is_valid_text(params["partitions_config"]):
            logger.error("Partitions configuration is required for partitioning.")
            raise ValueError("Partitions configuration is required for partitioning.")
        
        # Create partitions
        run_notebook(
            PARTITIONER_NOTEBOOK_NAME,
            params["notebook_timeout"],
            {"workspace_id": params["workspace_id"], "dataset_id": params["dataset_id"], "partitions_config": params["partitions_config"]}
        )
    else:
        logger.info("Partition creation is disabled.")
    
    # Refresh dataset if enable_refresh flag is enabled
    if params["enable_refresh"]:
        
        logger.info("Refresh dataset is enabled.")
        objects: Optional[str] = None

        # Check for explicit refresh configuration
        if is_valid_text(params["partitions_to_refresh"]):
            objects = params["partitions_to_refresh"]
            logger.info(f"Using provided list of partitions to refresh: {params['partitions_to_refresh']}")
        # Generate refresh list because refresh configuration not explicitly provided
        elif is_valid_text(params["partitions_config"]):
            try:
                partitions_config_df : pd.DataFrame = pd.read_json(StringIO(params["partitions_config"]))
                logger.info(f"Creating a list of partitions to refresh for tables: {partitions_config_df['table'].tolist()}\n")
                objects = generate_partitions_list(partitions_config_df)
                logger.info(f"Partitions to refresh:\n{objects}\n")
            except Exception as e:
                logger.error(f"Failed to process refresh configuration: {str(e)}")
                raise
        else:
            logger.warning("No refresh information provided. All partitions will be refreshed.")
            
        # Run notebook to refresh dataset
        run_notebook(
            REFRESHER_NOTEBOOK_NAME,
            params["notebook_timeout"],
            {
                "workspace_id": params["workspace_id"], "dataset_id": params["dataset_id"], 
                "tables_to_refresh": params["tables_to_refresh"], "partitions_to_refresh": objects,
                "commit_mode": params["refresh_commit_mode"], "max_parallelism": params["refresh_max_parallelism"]
            }
        )
        logger.info("Dataset refresh completed successfully.")
    else:
        logger.info("Refresh dataset is disabled.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

if __name__ == "__main__":
    run()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
