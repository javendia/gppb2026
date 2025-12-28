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

workspace_id = ""
dataset_id = ""
enable_partition = True
partitions_config = ""
enable_refresh = True
tables_to_refresh = ""
partitions_to_refresh = ""
refresh_config = ""
commit_mode = "transactional"
max_parallelism = 4

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
DEFAULT_NOTEBOOK_TIMEOUT = 3600

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

get_ipython().run_line_magic("pip", f"install /synfs/nb_resource/builtin/fabtoolkit-{FABTTOOLKIT_VERSION}-py3-none-any.whl")

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
    dataframe_to_str,
    Constants
)
from fabtoolkit.log import ConsoleFormatter
from fabtoolkit.dataset import Dataset

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
    console_handler.setFormatter(ConsoleFormatter())
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

def generate_partitions_list(partitions: pd.DataFrame) -> str:
    """
    Generates a JSON-formatted string representing partition ranges for each table in the input DataFrame.

    Args:
        partitions (pd.DataFrame): DataFrame containing ['table', 'first_date', 'interval', 'refresh_interval', 'number_of_intervals'] columns.

    Returns:
        str: JSON-formatted string with partitions separated by commas for each table.

    Raises:
        Exception: If any step in the process fails.
    """
    date_format = Constants.DATE_FORMAT
    logger.info("Calculating bounds for each table...")
    try:
        end_date = datetime.today()
        first_dates = pd.to_datetime(partitions["first_date"], format=date_format)
        # Getting bounds using refresh interval
        bounds = partitions.apply(lambda row: get_bounds_from_offset(
            first_dates.loc[row.name], end_date, row["refresh_interval"], row["number_of_intervals"]), axis=1, result_type='expand')
        partitions[["start_date", "end_date"]] = bounds
    except Exception as e:
        logger.error(f"Unable to calculate bounds for partitions: {str(e)}")
        raise
    
    date_ranges = []
    logger.info("Generating date ranges for each table...")
    for row in partitions.itertuples():
        try:
            # Generates a list of date ranges with the interval used to create table partitions.
            date_range = generate_date_ranges(row.start_date, row.end_date, row.interval).assign(table=row.table)
            date_ranges.append(date_range)
        except Exception as e:
            logger.error(f"Unable to generate date ranges for partitions: {str(e)}")
            raise
    partitions = pd.concat(date_ranges, ignore_index=True)
    logger.info("Composing partition names to refresh...")
    
    try:
        # Generating partitions list like Table_yyyyMMdd_yyyyMM_dd
        partitions["range_start"] = pd.to_datetime(partitions["range_start"]).dt.strftime(date_format)
        partitions["range_end"] = pd.to_datetime(partitions["range_end"]).dt.strftime(date_format)
        partitions["partition"] = partitions["table"] + '_' + partitions["range_start"] + '_' + partitions["range_end"]
        
        partitions_agg = (
            partitions.groupby("table")["partition"]
            .agg(','.join)
            .reset_index()
            .rename(columns={"partition": "selected_partitions"})
        )

        objects = dataframe_to_str(partitions_agg)
        logger.info(f"Partitions list created successfully.")
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

def run_notebook(notebook_name: str, params: Dict[str, Any]) -> None:
    """
    Runs a Fabric notebook with specified parameters and timeout.

    Args:
        notebook_name (str): Name of the notebook to run.
        params (Dict[str, Any]): Dictionary of parameters to pass to the notebook.

    Returns:
        None

    Raises:
        RuntimeError: If notebook execution fails.
    """
    try:
        logger.info(f"Running notebook '{notebook_name}'...")
        notebookutils.notebook.run(notebook_name, DEFAULT_NOTEBOOK_TIMEOUT, params)
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
    
    # Create partitions if enable_partition flag is enabled
    if enable_partition:
        logger.info("Partition dataset is enabled.")
        
        # Create partitions
        run_notebook(
            PARTITIONER_NOTEBOOK_NAME,
            {"workspace_id": workspace_id, "dataset_id": dataset_id, "partitions_config": partitions_config}
        )
    else:
        logger.info("Partition creation is disabled.")
    
    # Refresh dataset if enable_refresh flag is enabled
    if enable_refresh:
        logger.info("Refresh dataset is enabled.")
        objects: Optional[str] = None

        # Check for explicit refresh configuration
        if is_valid_text(partitions_to_refresh):
            objects = partitions_to_refresh
            logger.info(f"Using provided list of partitions to refresh: {partitions_to_refresh}")
        # Generate refresh list because refresh configuration not explicitly provided
        elif is_valid_text(refresh_config):
            logger.info("Validating refresh configuration...")
            columns = ["table", "first_date", "interval", "refresh_interval", "number_of_intervals"]
            refresh_config_df = validate_json(refresh_config, columns)
            try:
                logger.info(f"Creating a list of partitions to refresh for tables: {refresh_config_df['table'].tolist()}")
                objects = generate_partitions_list(refresh_config_df)
            except Exception as e:
                logger.error(f"Failed to process refresh configuration: {str(e)}")
                raise
        else:
            logger.info("No refresh information provided. All partitions will be refreshed.")

        # Run notebook to refresh dataset
        run_notebook(
            REFRESHER_NOTEBOOK_NAME,
            {
                "workspace_id": workspace_id, "dataset_id": dataset_id, 
                "tables_to_refresh": tables_to_refresh, "partitions_to_refresh": objects,
                "commit_mode": commit_mode, "max_parallelism": max_parallelism
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
