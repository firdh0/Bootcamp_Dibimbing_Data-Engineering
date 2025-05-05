import pandas as pd
import yaml

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from scripts.utils.logger import Logger 

class DataExtractor:
    """
    A class for extracting data from CSV files and saving it into a staging area.

    This class loads a configuration from a YAML file and provides methods to extract data from a CSV file
    and save it in a Parquet format in the staging area.

    Methods:
        __init__(config_path: str) -> None:
            Initializes the DataExtractor instance with a configuration path.

        load_config() -> dict:
            Loads the configuration from the YAML file.

        extract_data(csv_file: str, staging_file: str) -> bool:
            Extracts data from a CSV file and saves it in Parquet format to the staging area.
    """

    def __init__(self, config_path: str) -> None:
        """
        Initialize the DataExtractor instance.

        Parameters:
            config_path (str): Path to the YAML configuration file.

        Returns:
            None
        """
        self.config_path = config_path
        self.logger = Logger(__name__).get_logger()

    def load_config(self) -> dict:
        """
        Loads the configuration from the YAML file.

        Parameters:
            None

        Returns:
            dict: The parsed configuration data.

        Raises:
            Exception: If the YAML file cannot be loaded.
        """
        try:
            with open(self.config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            self.logger.error(f"Gagal memuat konfigurasi: {e}")
            raise

    def extract_data(self, csv_file: str, staging_file: str) -> bool:
        """
        Extracts data from a CSV file and saves it in Parquet format to the staging area.

        Parameters:
            csv_file (str): The path to the CSV file to be extracted.
            staging_file (str): The path where the extracted data should be saved in Parquet format.

        Returns:
            bool: Returns `True` if the data was successfully extracted and saved, `False` otherwise.

        Raises:
            Exception: If there is an error during the extraction or saving process.
        """
        try:
            data = pd.read_csv(csv_file)
            self.logger.info("Data berhasil diekstrak dari CSV.")
            data.to_parquet(staging_file, index=False)
            self.logger.info(f"Data berhasil disimpan ke staging area ({staging_file}).")
            return True
        except Exception as e:
            self.logger.error(f"Gagal mengekstrak data: {e}")
            return False

# if __name__ == "__main__":
#     # Contoh penggunaan
#     config_path = './config/db_config.yml'
#     extractor = DataExtractor(config_path)
#     config = extractor.load_config()
#     base_path = config.get('base_path', './')  # Menggunakan base_path dari konfigurasi atau default ke ./
    
#     csv_file = f"{base_path}data/raw/bbc_news.csv"
#     staging_file = f"{base_path}data/processed/staging_data.parquet"
    
#     success = extractor.extract_data(csv_file, staging_file)
#     if not success:
#         sys.exit(1)

# PS C:\Users\LEGION\Videos\DIBIMBING\Day 10 - Assignment> python .\scripts\etl\extract.py