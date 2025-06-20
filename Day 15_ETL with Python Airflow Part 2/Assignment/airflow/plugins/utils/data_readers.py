from __future__ import annotations

import logging
import pandas as pd
from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)

def read_csv_file(file_path: str) -> pd.DataFrame | None:
    """Reads a CSV file and returns it as a Pandas DataFrame."""
    log.info(f"Reading CSV file from: {file_path}")
    try:
        df = pd.read_csv(file_path)
        log.info(f"Successfully read {len(df)} rows from CSV: {file_path}")
        return df
    except FileNotFoundError:
        log.error(f"CSV file not found at path: {file_path}")
        raise
    except Exception as e:
        log.error(f"Error reading CSV file {file_path}: {e}")
        raise

def read_json_file(file_path: str) -> pd.DataFrame | None:
    """Reads a JSON file (list of records) and returns it as a Pandas DataFrame."""
    log.info(f"Reading JSON file from: {file_path}")
    try:
        df = pd.read_json(file_path, orient='records', lines=False)
        log.info(f"Successfully read {len(df)} rows from JSON: {file_path}")
        return df
    except FileNotFoundError:
        log.error(f"JSON file not found at path: {file_path}")
        raise
    except Exception as e:
        log.error(f"Error reading JSON file {file_path}: {e}")
        raise