# etl_project/scripts/utils/db_connection.py
import psycopg2
from sqlalchemy import create_engine
import yaml

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from scripts.utils.logger import Logger 

class DatabaseConnection:
    """
    A class for managing PostgreSQL database connections using both psycopg2 and SQLAlchemy.

    This class provides methods to load configuration from a YAML file, establish connections 
    with psycopg2 for raw SQL operations, and SQLAlchemy for ORM or advanced queries.
    It also supports context manager protocol for automatic connection management.

    Methods:
        __init__(config_path: str) -> None:
            Initializes the DatabaseConnection instance with a configuration path.
            
        load_db_config() -> dict:
            Loads database configuration from a YAML file.

        connect() -> tuple[psycopg2.extensions.connection, sqlalchemy.engine.base.Engine]:
            Establishes and returns both psycopg2 and SQLAlchemy connections.

        close() -> None:
            Closes the psycopg2 connection.

        __enter__() -> tuple[psycopg2.extensions.connection, sqlalchemy.engine.base.Engine]:
            Enables usage with context managers (with statement).

        __exit__(exc_type, exc_val, exc_tb) -> None:
            Automatically closes the connection at the end of a with block.
    """

    def __init__(self, config_path: str) -> None:
        """
        Initialize the DatabaseConnection instance.

        Parameters:
            config_path (str): The path to the YAML configuration file.

        Returns:
            None
        """
        self.config_path = config_path
        self.connection = None
        self.engine = None
        self.logger = Logger(__name__).get_logger()  # Inisialisasi logger

    def load_db_config(self) -> dict:
        """
        Load the database configuration from the YAML file.

        Returns:
            dict: A dictionary containing database connection settings.

        Raises:
            Exception: If the configuration file cannot be read or parsed.
        """
        try:
            with open(self.config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            self.logger.error(f"Gagal memuat konfigurasi database: {e}")
            raise

    def connect(self) -> tuple:
        """
        Establish connections to the PostgreSQL database using psycopg2 and SQLAlchemy.

        Returns:
            tuple: A tuple containing:
                - psycopg2 connection object
                - SQLAlchemy engine object

        Raises:
            Exception: If the connection attempt fails.
        """
        try:
            config = self.load_db_config()
            db_config = config['postgresql']

            # Koneksi menggunakan psycopg2
            self.connection = psycopg2.connect(
                host=db_config['host'],
                database=db_config['database'],
                user=db_config['user'],
                password=db_config['password']
            )
            self.logger.info("Koneksi database berhasil dibuka.")

            # Koneksi menggunakan SQLAlchemy untuk ORM dan operasi tingkat tinggi
            conn_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
            self.engine = create_engine(conn_string)
            return self.connection, self.engine
        except Exception as e:
            self.logger.error(f"Gagal menyambungkan ke database: {e}")
            raise

    def close(self) -> None:
        """
        Close the psycopg2 database connection.

        Returns:
            None

        Raises:
            Exception: If closing the connection fails.
        """
        if self.connection:
            try:
                self.connection.close()
                self.logger.info("Koneksi database berhasil ditutup.")
            except Exception as e:
                self.logger.error(f"Gagal menutup koneksi database: {e}")
                raise

    def __enter__(self) -> tuple:
        """
        Support for context manager entry. Calls `connect()`.

        Returns:
            tuple: The result of the `connect()` method.
        """
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Support for context manager exit. Calls `close()`.

        Parameters:
            exc_type: Exception type, if raised.
            exc_val: Exception value, if raised.
            exc_tb: Traceback, if raised.

        Returns:
            None
        """
        self.close()

# Contoh penggunaan
# if __name__ == "__main__":
#     db = DatabaseConnection('./config/db_config.yml')
#     conn, engine = db.connect()
#     db.close()

# PS C:\Users\LEGION\Videos\DIBIMBING\Day 10 - Assignment> python .\scripts\utils\db_connection.py