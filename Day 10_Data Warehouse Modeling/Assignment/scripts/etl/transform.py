import pandas as pd
import yaml

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from scripts.utils.logger import Logger 
from scripts.utils.data_quality import DataQuality

class DataTransformer:
    """
    A class for transforming and processing data loaded from a staging file.

    This class provides methods to load configuration, read data from a staging area (parquet file),
    and perform data transformation including data quality checks (duplicates, null values, data types, etc.).

    Methods:
        __init__(config_path: str) -> None:
            Initializes the DataTransformer instance with a configuration path.
            
        load_config() -> dict:
            Loads the configuration from the YAML file.
        
        process_data(staging_file: str) -> pd.DataFrame | None:
            Reads data from the given staging file (parquet).
        
        transform_data(data: pd.DataFrame) -> pd.DataFrame | None:
            Transforms the data by performing various quality checks and validation.
    """

    def __init__(self, config_path: str) -> None:
        """
        Initialize the DataTransformer instance.

        Parameters:
            config_path (str): Path to the YAML configuration file.

        Returns:
            None
        """
        self.config_path = config_path
        self.logger = Logger(__name__).get_logger()
        self.dq = None

    def load_config(self) -> dict:
        """
        Load the configuration from the YAML file.

        Returns:
            dict: A dictionary containing the configuration settings.

        Raises:
            Exception: If the configuration file cannot be loaded or parsed.
        """
        try:
            with open(self.config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            self.logger.error(f"Gagal memuat konfigurasi: {e}")
            raise

    def process_data(self, staging_file: str) -> pd.DataFrame | None:
        """
        Reads data from the given staging file (parquet).

        Parameters:
            staging_file (str): The path to the parquet file containing the staging data.

        Returns:
            pd.DataFrame | None: Returns the loaded data as a DataFrame, or None if loading fails.
        """
        try:
            data = pd.read_parquet(staging_file)
            self.logger.info("Data berhasil dibaca dari staging area.")
            return data
        except Exception as e:
            self.logger.error(f"Gagal memproses data dari staging area: {e}")
            return None

    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame | None:
        """
        Transforms the data by performing various quality checks and validation.

        This includes checking for duplicates, null values, and validating data types and ranges.

        Parameters:
            data (pd.DataFrame): The data to be transformed.

        Returns:
            pd.DataFrame | None: Returns the transformed data as a DataFrame, or None if transformation fails.
        """
        try:
            # Inisialisasi pemeriksa kualitas data
            self.dq = DataQuality({'bbc_df': data})
            
            # Periksa duplikat
            duplicates_df = self.dq.check_duplicates()
            self.dq.handle_duplicates(duplicates_df)

            # Periksa nilai null
            null_values_df = self.dq.check_null_values()
            self.dq.handle_null_values(null_values_df, drop_null_threshold=25)

            # Validasi tipe data dan rentang nilai
            expected_data_types = {
                'year': 'int64',
                'day': 'int64',
                'month': 'int64'
            }
            value_ranges = {
                'year': (1900, 2100),
                'day': (1, 31),
                'month': (1, 12)
            }
            validation_df = self.dq.validate_data_types_and_ranges(expected_data_types, value_ranges)
            self.logger.info("\n" + validation_df.to_string())

            return data
        except Exception as e:
            self.logger.error(f"Gagal mentransformasi data: {e}")
            return None

# if __name__ == "__main__":
#     # Contoh penggunaan
#     config_path = './config/db_config.yml'
#     transformer = DataTransformer(config_path)
#     config = transformer.load_config()
#     base_path = config.get('base_path', './')  # Menggunakan base_path dari konfigurasi atau default ke ./
#     staging_file = f"{base_path}data/processed/staging_data.parquet"

#     data = transformer.process_data(staging_file)
#     if data is not None:
#         transformed_data = transformer.transform_data(data)
#         # Simpan data yang telah di-transformasi jika diperlukan
#         transformed_data.to_parquet(f"{base_path}data/processed/transformed_data.parquet", index=False)
#         transformer.logger.info("Data berhasil disimpan setelah transformasi.")

# PS C:\Users\LEGION\Videos\DIBIMBING\Day 10 - Assignment> python .\scripts\etl\transform.py