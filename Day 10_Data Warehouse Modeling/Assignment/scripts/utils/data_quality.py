# etl_project/scripts/utils/data_quality.py
import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple, Any
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from scripts.utils.logger import Logger 

class DataQuality:
    """
    A class to perform various data quality checks and cleaning operations
    on a collection of datasets.

    Methods:
        __init__(datasets: Dict[str, pd.DataFrame]) -> None:
            Initialize the DataQuality class with datasets.
            
        check_duplicates() -> pd.DataFrame:
            Check for duplicate rows in each dataset and return a DataFrame of duplicates.
        
        check_null_values() -> pd.DataFrame:
            Check for null values in each dataset and return a summary DataFrame.

        handle_duplicates(duplicates_df: pd.DataFrame) -> None:
            Remove duplicate rows from datasets based on 'url' column.

        impute_value(df: pd.DataFrame, column_name: str, method: str, dataset_name: str) -> None:
            Impute missing values in a specified column using a selected method.

        handle_null_values(null_values_df: pd.DataFrame, drop_null_threshold: float = 20.0) -> None:
            Handle missing values by dropping or imputing based on threshold and data type.

        validate_data_types_and_ranges(expected_types: Optional[Dict[str, Any]] = None,
                                        value_ranges: Optional[Dict[str, Tuple[float, float]]] = None) -> pd.DataFrame:
            Validate data types and check value ranges of columns.
    """

    def __init__(self, datasets: Dict[str, pd.DataFrame]) -> None:
        """
        Initialize the DataQuality class with datasets.

        Parameters:
            datasets (Dict[str, pd.DataFrame]): Dictionary mapping dataset names to DataFrames.
        """
        self.datasets = datasets
        self.logger = Logger(__name__).get_logger()

    def check_duplicates(self) -> pd.DataFrame:
        """
        Check for duplicate rows in each dataset.

        Returns:
            pd.DataFrame: Combined DataFrame listing all duplicated rows across datasets.
        """
        combined_duplicates = pd.DataFrame(columns=['Dataset'])  # Inisialisasi dengan kolom 'Dataset'
        for name, df in self.datasets.items():
            duplicate_count = df.duplicated(keep='first').sum()
            if duplicate_count > 0:
                self.logger.info(f"Dataset '{name}' memiliki {duplicate_count} baris duplikat.")
                duplicates_df = df[df.duplicated(keep='first')].copy()
                duplicates_df['Dataset'] = name
                duplicates_df['Jumlah Duplikasi'] = df.groupby(list(df.columns)).transform('size')
                cols = ['Dataset'] + [col for col in duplicates_df.columns if col != 'Dataset']
                duplicates_df = duplicates_df[cols]
                combined_duplicates = pd.concat([combined_duplicates, duplicates_df], ignore_index=True)
        return combined_duplicates

    def check_null_values(self) -> pd.DataFrame:
        """
        Check for null values in each dataset and summarize them.

        Returns:
            pd.DataFrame: Summary of null values per column and dataset.
        """
        combined_nulls = pd.DataFrame()
        for name, df in self.datasets.items():
            df = self._convert_to_datetime(df)
            null_values = df.isnull().sum()
            null_columns = null_values[null_values > 0]
            if not null_columns.empty:
                self.logger.info(f"Dataset '{name}' memiliki null values.")
                column_types = df.dtypes[null_columns.index]
                is_date_type = column_types.apply(pd.api.types.is_datetime64_any_dtype)
                null_value_summary = pd.DataFrame({
                    'Dataset': name,
                    'Kolom': null_columns.index,
                    'Tipe Data': column_types.values,
                    'Jumlah Null Values': null_columns.values,
                    'Persentase Null Values (%)': (null_columns.values / len(df)) * 100,
                    'Apakah Tipe Data Date?': is_date_type.values
                })
                combined_nulls = pd.concat([combined_nulls, null_value_summary], ignore_index=True)
        return combined_nulls

    def _convert_to_datetime(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert columns with 'date' or 'timestamp' in name to datetime.

        Parameters:
            df (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with converted datetime columns.
        """
        date_columns = [col for col in df.columns if 'date' in col.lower() or 'timestamp' in col.lower()]
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        return df

    def handle_duplicates(self, duplicates_df: pd.DataFrame) -> None:
        """
        Remove duplicate rows from datasets based on 'url' column.

        Parameters:
            duplicates_df (pd.DataFrame): DataFrame containing duplicate information.

        Returns:
            None
        """
        if 'Dataset' not in duplicates_df.columns:
            self.logger.info("Tidak ada duplikat untuk ditangani.")
            return
        datasets_with_duplicates = duplicates_df['Dataset'].unique()
        for dataset_name in datasets_with_duplicates:
            if dataset_name in self.datasets:
                self.logger.info(f"Menghapus duplikat dari dataset '{dataset_name}'.")
                self.datasets[dataset_name] = self.datasets[dataset_name].drop_duplicates(subset='url', keep='first')

    def impute_value(self, df: pd.DataFrame, column_name: str, method: str, dataset_name: str) -> None:
        """
        Impute missing values in a specified column using a selected method.

        Parameters:
            df (pd.DataFrame): DataFrame to process.
            column_name (str): Column to impute.
            method (str): Imputation method ('mean', 'median', 'mode', 'interpolate').
            dataset_name (str): Name of the dataset for logging.

        Returns:
            None

        Raises:
            ValueError: If the method is not recognized.
        """
        if method == 'mean':
            df[column_name] = df[column_name].fillna(df[column_name].mean())
        elif method == 'median':
            df[column_name] = df[column_name].fillna(df[column_name].median())
        elif method == 'mode':
            df[column_name] = df[column_name].fillna(df[column_name].mode()[0])
        elif method == 'interpolate':
            df[column_name] = df[column_name].interpolate()
            df[column_name] = df[column_name].dt.floor('s')
        else:
            self.logger.error(f"Metode imputasi '{method}' tidak dikenali.")
            raise ValueError(f"Imputation method '{method}' is not recognized.")
        self.logger.info(f"Imputasi dilakukan pada kolom '{column_name}' dari dataset '{dataset_name}' menggunakan metode '{method}'.")

    def handle_null_values(self, null_values_df: pd.DataFrame, drop_null_threshold: float = 20.0) -> None:
        """
        Handle missing values by dropping or imputing based on threshold and data type.

        Parameters:
            null_values_df (pd.DataFrame): Summary of null values (output of `check_null_values()`).
            drop_null_threshold (float): Threshold percentage for dropping columns.

        Returns:
            None
        """
        for _, row in null_values_df.iterrows():
            dataset_name = row['Dataset']
            column_name = row['Kolom']
            null_percentage = row['Persentase Null Values (%)']
            df = self.datasets.get(dataset_name)
            if df is None:
                self.logger.error(f"Dataset '{dataset_name}' tidak ditemukan.")
                continue
            if null_percentage > drop_null_threshold:
                if column_name in df.columns:
                    df.drop(column_name, axis=1, inplace=True)
                    self.logger.info(f"Kolom '{column_name}' pada dataset '{dataset_name}' telah di-drop karena persentase null ({null_percentage}%) melebihi threshold.")
                else:
                    self.logger.error(f"Kolom '{column_name}' tidak ditemukan dalam dataset '{dataset_name}' dan tidak dapat di-drop.")
                    continue
            elif column_name in df.columns:
                if pd.api.types.is_numeric_dtype(df[column_name]):
                    skewness_value = df[column_name].dropna().skew()
                    method = 'median' if skewness_value != 0 else 'mean'
                    self.impute_value(df, column_name, method, dataset_name)
                elif pd.api.types.is_datetime64_any_dtype(df[column_name]):
                    self.impute_value(df, column_name, 'interpolate', dataset_name)
                else:
                    self.impute_value(df, column_name, 'mode', dataset_name)
            else:
                self.logger.error(f"Kolom '{column_name}' tidak ada dalam dataset '{dataset_name}', lanjutkan ke kolom berikutnya.")
                continue

    def validate_data_types_and_ranges(
        self,
        expected_types: Optional[Dict[str, Any]] = None,
        value_ranges: Optional[Dict[str, Tuple[float, float]]] = None
    ) -> pd.DataFrame:
        """
        Validate data types and check value ranges of columns.

        Parameters:
            expected_types (Optional[Dict[str, Any]]): Expected data types for columns.
            value_ranges (Optional[Dict[str, Tuple[float, float]]]): Acceptable min-max ranges for numeric columns.

        Returns:
            pd.DataFrame: DataFrame summarizing validation results.
        """
        if expected_types is None:
            expected_types = {}
        if value_ranges is None:
            value_ranges = {}
        validation_results = []
        for name, df in self.datasets.items():
            for column in df.columns:
                if column in expected_types:
                    expected_type = expected_types[column]
                    actual_type = df[column].dtype
                    type_match = actual_type == expected_types[column]
                    validation_results.append({
                        'Dataset': name,
                        'Column': column,
                        'Check': 'Data Type',
                        'Expected': expected_type,
                        'Actual': actual_type,
                        'Status': 'Pass' if type_match else 'Fail'
                    })
                if column in value_ranges and pd.api.types.is_numeric_dtype(df[column]):
                    min_val, max_val = value_ranges[column]
                    if not ((df[column] >= min_val) & (df[column] <= max_val)).all():
                        validation_results.append({
                            'Dataset': name,
                            'Column': column,
                            'Check': 'Value Range',
                            'Expected': f"{min_val} to {max_val}",
                            'Actual': "Out of range values exist",
                            'Status': 'Fail'
                        })
                    else:
                        validation_results.append({
                            'Dataset': name,
                            'Column': column,
                            'Check': 'Value Range',
                            'Expected': f"{min_val} to {max_val}",
                            'Actual': "All values in range",
                            'Status': 'Pass'
                        })
        return pd.DataFrame(validation_results)

# Contoh penggunaan
# if __name__ == "__main__":
#     # Contoh data
#     data = {
#         'id': [1, 2, 3, 4, 5],
#         'name': ['Alice', 'Bob', None, 'Dave', 'Eve'],
#         'age': [25, 30, 35, None, 40],
#         'signup_date': ['2022-01-01', '2022-02-01', '2022-03-01', '2022-04-01', '2022-05-01']
#     }
#     df = pd.DataFrame(data)
#     datasets = {'example': df}

#     dq = DataQuality(datasets)
#     duplicates = dq.check_duplicates()
#     nulls = dq.check_null_values()
#     dq.handle_duplicates(duplicates)
#     dq.handle_null_values(nulls)
#     validation = dq.validate_data_types_and_ranges(
#         expected_types={'id': 'int64', 'name': 'object', 'age': 'float64', 'signup_date': 'datetime64[ns]'},
#         value_ranges={'age': (18, 100)}
#     )
#     print("Duplicates:\n", duplicates)
#     print("\nNull Values:\n", nulls)
#     print("\nValidation Results:\n", validation)

    # PS C:\Users\LEGION\Videos\DIBIMBING\Day 10 - Assignment> python .\scripts\utils\data_quality.py