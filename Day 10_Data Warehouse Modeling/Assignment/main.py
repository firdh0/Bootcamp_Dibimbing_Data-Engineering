# etl_project/scripts/main.py
import yaml

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from scripts.utils.logger import Logger 
from scripts.utils.db_connection import DatabaseConnection 
from scripts.etl.extract import DataExtractor 
from scripts.etl.transform import DataTransformer 
from scripts.etl.load import DataLoader  

class DataPipeline:
    def __init__(self, config_path):
        self.config_path = config_path
        self.logger = Logger(__name__).get_logger()
        self.base_path = None
        self.config = None
        self.db_connection = None

    def load_config(self):
        try:
            with open(self.config_path, 'r') as file:
                self.config = yaml.safe_load(file)
                self.base_path = self.config.get('base_path', './')
                self.logger.info("Konfigurasi berhasil dimuat.")
        except Exception as e:
            self.logger.error(f"Gagal memuat konfigurasi: {e}")
            sys.exit(1)

    def initialize_db_connection(self):
        try:
            self.db_connection = DatabaseConnection(self.config_path)
            self.db_connection.connect()
            self.logger.info("Koneksi database berhasil dibuka.")
        except Exception as e:
            self.logger.error(f"Gagal menginisialisasi koneksi database: {e}")
            sys.exit(1)

    def close_db_connection(self):
        try:
            if self.db_connection:
                self.db_connection.close()
                self.logger.info("Koneksi database berhasil ditutup.")
        except Exception as e:
            self.logger.error(f"Gagal menutup koneksi database: {e}")
            sys.exit(1)

    def run(self):
        self.load_config()
        self.initialize_db_connection()

        # Proses Ekstraksi
        extractor = DataExtractor(self.config_path)
        csv_file = f"{self.base_path}data/raw/bbc_news.csv"
        staging_file = f"{self.base_path}data/processed/staging_data.parquet"
        success = extractor.extract_data(csv_file, staging_file)
        if not success:
            self.logger.error("Proses ekstraksi gagal. Keluar dari pipeline.")
            self.close_db_connection()
            sys.exit(1)

        # Proses Transformasi
        transformer = DataTransformer(self.config_path)
        data = transformer.process_data(staging_file)
        if data is not None:
            transformed_data = transformer.transform_data(data)
            transformed_data.to_parquet(f"{self.base_path}data/processed/transformed_data.parquet", index=False)
            self.logger.info("Data berhasil di-transformasi dan disimpan.")
        else:
            self.logger.error("Proses transformasi gagal. Keluar dari pipeline.")
            self.close_db_connection()
            sys.exit(1)

        # Proses Pemuatan
        loader = DataLoader(self.config_path)
        loader.db_connection = self.db_connection  # Menggunakan koneksi yang sama
        success_dim = loader.load_to_dimension_tables(transformed_data)
        if not success_dim:
            self.logger.error("Proses pemuatan ke tabel dimensi gagal. Keluar dari pipeline.")
            self.close_db_connection()
            sys.exit(1)

        success_fact = loader.load_to_fact_table(transformed_data)
        if not success_fact:
            self.logger.error("Proses pemuatan ke tabel fakta gagal. Keluar dari pipeline.")
            self.close_db_connection()
            sys.exit(1)

        self.close_db_connection()
        self.logger.info("Proses ETL berhasil selesai.")

if __name__ == "__main__":
    # Contoh penggunaan
    config_path = './config/db_config.yml'
    pipeline = DataPipeline(config_path)
    pipeline.run()