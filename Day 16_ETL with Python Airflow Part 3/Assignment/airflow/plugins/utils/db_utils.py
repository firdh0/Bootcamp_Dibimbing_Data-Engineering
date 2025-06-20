from __future__ import annotations

import logging
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

def get_or_create_dimension_id(
    hook: PostgresHook, 
    table_name: str, 
    key_columns: dict, 
    id_column_name: str
) -> int | None:
    """
    Looks up or creates a dimension row and returns its ID.
    Assumes the ID column is SERIAL and auto-generates.

    Parameters:
        hook: PostgresHook instance.
        table_name: Name of the dimension table.
        key_columns: Dictionary of column names and their values that define the natural key.
        id_column_name: Name of the primary key ID column.

    Returns:
        The ID of the existing or newly created dimension row, or None if an error occurs.
    """
    condition_clauses = []
    condition_values = []
    
    # Build the WHERE clause for the SELECT statement
    # Enclose column names in double quotes for safety, especially if they are reserved keywords or contain special characters.
    for col, val in key_columns.items():
        if pd.isna(val): # Handle Python None or Pandas NaT/NaN
            condition_clauses.append(f"\"{col}\" IS NULL") # Enclose column names in double quotes for safety
        else:
            condition_clauses.append(f"\"{col}\" = %s") # Enclose column names
            condition_values.append(val)
    
    if not condition_clauses:
        log.error(f"No key columns provided for dimension lookup in table '{table_name}'.")
        return None

    select_sql = f"SELECT \"{id_column_name}\" FROM {table_name} WHERE {' AND '.join(condition_clauses)};"
    
    try:
        # Execute the SELECT query
        result = hook.get_first(select_sql, parameters=tuple(condition_values) if condition_values else None)
        
        if result and result[0] is not None:
            log.debug(f"Found existing ID {result[0]} in {table_name} for keys: {key_columns}")
            return result[0]
        else:
            # If not found, insert a new dimension row
            log.info(f"No existing record found in {table_name} for {key_columns}. Inserting new record.")
            insert_cols_list = []
            insert_placeholders_list = []
            insert_values_list = []
            
            for col, val in key_columns.items():
                insert_cols_list.append(f"\"{col}\"") # Enclose column names
                insert_placeholders_list.append("%s")
                # Convert pandas Timestamp to Python datetime if necessary for the hook
                if isinstance(val, pd.Timestamp):
                    insert_values_list.append(val.to_pydatetime())
                elif pd.isna(val): # Ensure NaT/None are passed as SQL NULL
                    insert_values_list.append(None)
                else:
                    insert_values_list.append(val)

            insert_cols_str = ", ".join(insert_cols_list)
            insert_placeholders_str = ", ".join(insert_placeholders_list)
            
            # Construct the INSERT statement with RETURNING clause to get the new ID
            insert_sql = f"INSERT INTO {table_name} ({insert_cols_str}) VALUES ({insert_placeholders_str}) RETURNING \"{id_column_name}\";"
            
            # Execute the INSERT statement
            # The `run` method with `returning` typically returns a list of tuples/lists.
            inserted_id_result = hook.run(insert_sql, autocommit=True, parameters=tuple(insert_values_list))
            
            if inserted_id_result and isinstance(inserted_id_result, list) and \
               len(inserted_id_result) > 0 and inserted_id_result[0] and \
               isinstance(inserted_id_result[0], (list, tuple)) and \
               len(inserted_id_result[0]) > 0 and inserted_id_result[0][0] is not None:
                new_id = inserted_id_result[0][0]
                log.info(f"Inserted into {table_name}: {key_columns}, got new ID: {new_id}")
                return new_id
            else: 
                # Fallback: If RETURNING didn't work as expected (e.g., driver differences or no rows returned)
                log.warning(f"Could not retrieve ID directly using RETURNING for {table_name} with {key_columns}. Insert result: {inserted_id_result}. Attempting re-select.")
                result_after_insert = hook.get_first(select_sql, parameters=tuple(condition_values) if condition_values else None)
                if result_after_insert and result_after_insert[0] is not None:
                    log.info(f"Re-selected ID {result_after_insert[0]} after insert for {table_name}.")
                    return result_after_insert[0]
                else:
                    log.error(f"Failed to get ID even after re-selecting for {table_name} with {key_columns}. This indicates a problem with the insert or data consistency.")
                    return None # Critical failure to get ID
                    
    except Exception as e:
        log.error(f"Error in get_or_create_dimension_id for table '{table_name}' with keys {key_columns}: {e}")
        log.exception(f"Detailed traceback for get_or_create_dimension_id error on table {table_name}:")
        return None
