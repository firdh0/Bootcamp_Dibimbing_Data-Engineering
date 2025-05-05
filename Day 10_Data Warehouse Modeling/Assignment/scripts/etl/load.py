# etl_project/scripts/etl/load.py
import pandas as pd
import yaml
from datetime import date

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from scripts.utils.logger import Logger 
from scripts.utils.db_connection import DatabaseConnection 

class DataLoader:
    """
    A class for loading data into dimension and fact tables in a database.

    This class provides methods to load data into various dimension tables (e.g., `dim_date`, `dim_category`, 
    `dim_source_category`, `dim_tag`, and `dim_article`) as well as a fact table (`fact_news`). It also includes
    a method for parsing date strings.

    Methods:
        __init__(config_path: str) -> None:
            Initializes the DataLoader instance with a configuration file path.
            
        parse_date(date_str: str) -> date:
            Parses a date string into a date object, using day-first format.

        load_to_dimension_tables(data: pd.DataFrame) -> bool:
            Loads the provided data into various dimension tables in the database.

        load_to_fact_table(data: pd.DataFrame) -> bool:
            Loads the provided data into the fact table in the database.
    """

    def __init__(self, config_path: str) -> None:
        """
        Initialize the DataLoader instance.

        Parameters:
            config_path (str): Path to the YAML configuration file.

        Returns:
            None
        """
        self.config_path = config_path
        self.logger = Logger(__name__).get_logger()
        self.db_connection = None  # Akan diinisialisasi dari main.py

    def parse_date(self, date_str: str) -> date:
        """
        Parses a date string into a date object, using day-first format.

        Parameters:
            date_str (str): The date string to be parsed.

        Returns:
            date: The parsed date object.

        Raises:
            Exception: If the date string cannot be parsed.
        """
        try:
            return pd.to_datetime(date_str, format='%d-%m-%Y', dayfirst=True).date()
        except Exception:
            # fallback otomatis
            return pd.to_datetime(date_str, dayfirst=True).date()

    def load_to_dimension_tables(self, data: pd.DataFrame) -> bool:
        """
        Loads the provided data into various dimension tables in the database.

        This method inserts data into the `dim_date`, `dim_category`, `dim_source_category`, `dim_tag`, and 
        `dim_article` tables if the relevant records do not already exist.

        Parameters:
            data (pd.DataFrame): The data to be loaded into the dimension tables.

        Returns:
            bool: Returns `True` if the data was successfully loaded, `False` otherwise.

        Raises:
            Exception: If there is an error during the data insertion.
        """
        try:
            conn = self.db_connection.connection

            for _, row in data.iterrows():
                if pd.isna(row.get('updated')):
                    continue
                date_obj = self.parse_date(row['updated'])

                # dim_date
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO dim_date (date, day, month, year)
                        SELECT %s, EXTRACT(DAY FROM %s), EXTRACT(MONTH FROM %s), EXTRACT(YEAR FROM %s)
                        WHERE NOT EXISTS (SELECT 1 FROM dim_date WHERE date = %s)
                        """, (date_obj, date_obj, date_obj, date_obj, date_obj)
                    )
                    conn.commit()

                # dim_category
                if pd.notna(row.get('category')):
                    with conn.cursor() as cursor:
                        cursor.execute(
                            """
                            INSERT INTO dim_category (name)
                            SELECT %s
                            WHERE NOT EXISTS (SELECT 1 FROM dim_category WHERE name = %s)
                            """, (row['category'], row['category'])
                        )
                        conn.commit()

                # dim_source_category
                if pd.notna(row.get('source_category')):
                    with conn.cursor() as cursor:
                        cursor.execute(
                            """
                            INSERT INTO dim_source_category (name)
                            SELECT %s
                            WHERE NOT EXISTS (SELECT 1 FROM dim_source_category WHERE name = %s)
                            """, (row['source_category'], row['source_category'])
                        )
                        conn.commit()

                # dim_tag
                tags_field = row.get('tags')
                if pd.notna(tags_field):
                    tags = [t.strip() for t in tags_field.split(',') if t.strip()]
                    for tag in tags:
                        with conn.cursor() as cursor:
                            cursor.execute(
                                """
                                INSERT INTO dim_tag (name)
                                SELECT %s
                                WHERE NOT EXISTS (SELECT 1 FROM dim_tag WHERE name = %s)
                                """, (tag, tag)
                            )
                            conn.commit()

                # dim_article
                title = row.get('title')
                desc = row.get('description')
                url = row.get('url')
                if pd.notna(title) and pd.notna(desc) and pd.notna(url):
                    with conn.cursor() as cursor:
                        cursor.execute(
                            """
                            INSERT INTO dim_article (title, description, url)
                            SELECT %s, %s, %s
                            WHERE NOT EXISTS (SELECT 1 FROM dim_article WHERE url = %s)
                            """, (title, desc, url, url)
                        )
                        conn.commit()

            self.logger.info("Data dimensi berhasil dimuat.")
            return True

        except Exception as e:
            self.logger.error(f"Gagal memuat data dimensi: {e}")
            return False

    def load_to_fact_table(self, data: pd.DataFrame) -> bool:
        """
        Loads the provided data into the fact table in the database.

        This method inserts data into the `fact_news` table, linking it with the corresponding dimension tables
        (`dim_category`, `dim_source_category`, `dim_date`, `dim_article`, `dim_tag`). It also handles conflicts 
        by ensuring that the same record is not inserted multiple times.

        Parameters:
            data (pd.DataFrame): The data to be loaded into the fact table.

        Returns:
            bool: Returns `True` if the data was successfully loaded, `False` otherwise.

        Raises:
            Exception: If there is an error during the data insertion.
        """
        try:
            conn = self.db_connection.connection

            for _, row in data.iterrows():
                # Parsing tanggal untuk lookup
                if pd.isna(row.get('updated')):
                    self.logger.warning("Baris tanpa tanggal di fact_news dilewati.")
                    continue
                date_obj = self.parse_date(row['updated'])

                # Ambil IDs, skip jika tidak ditemukan
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT category_id FROM dim_category WHERE name = %s", (row.get('category'),)
                    )
                    res = cursor.fetchone()
                    if not res:
                        self.logger.warning(f"Kategori '{row.get('category')}' tidak ditemukan, baris dilewati.")
                        continue
                    category_id = res[0]

                    cursor.execute(
                        "SELECT source_category_id FROM dim_source_category WHERE name = %s", (row.get('source_category'),)
                    )
                    res = cursor.fetchone()
                    if not res:
                        self.logger.warning(f"Source category '{row.get('source_category')}' tidak ditemukan, baris dilewati.")
                        continue
                    source_category_id = res[0]

                    cursor.execute(
                        "SELECT date_id FROM dim_date WHERE date = %s", (date_obj,)
                    )
                    res = cursor.fetchone()
                    if not res:
                        self.logger.warning(f"Date '{date_obj}' tidak ditemukan di dim_date, baris dilewati.")
                        continue
                    date_id = res[0]

                    cursor.execute(
                        "SELECT news_id FROM dim_article WHERE url = %s", (row.get('url'),)
                    )
                    res = cursor.fetchone()
                    if not res:
                        self.logger.warning(f"Article URL '{row.get('url')}' tidak ditemukan, baris dilewati.")
                        continue
                    news_id = res[0]

                # Masukkan setiap tag ke fact_news
                tags_field = row.get('tags')
                if pd.notna(tags_field):
                    tags = [t.strip() for t in tags_field.split(',') if t.strip()]
                    for tag in tags:
                        with conn.cursor() as cursor:
                            cursor.execute(
                                "SELECT tag_id FROM dim_tag WHERE name = %s", (tag,)
                            )
                            res = cursor.fetchone()
                            if not res:
                                self.logger.warning(f"Tag '{tag}' tidak ditemukan, dilewati.")
                                continue
                            tag_id = res[0]

                            cursor.execute(
                                """
                                INSERT INTO fact_news (
                                    news_id, date_id, category_id, source_category_id, tag_id, is_live, in_pagination
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (news_id, date_id, category_id, source_category_id, tag_id) DO NOTHING
                                """,
                                (
                                    news_id, date_id, category_id, source_category_id, tag_id,
                                    row.get('live') == 'Yes', row.get('in_pagination', False)
                                )
                            )
                            conn.commit()

            self.logger.info("Data fakta berhasil dimuat.")
            return True

        except Exception as e:
            self.logger.error(f"Gagal memuat data fakta: {e}")
            return False

# if __name__ == "__main__":
#     # Contoh penggunaan
#     config_path = './config/db_config.yml'
#     loader = DataLoader(config_path)
#     config = yaml.safe_load(open(config_path, 'r'))
#     base_path = config.get('base_path', './') 
#     staging_file = f"{base_path}data/processed/staging_data.parquet"

#     data = pd.read_parquet(staging_file)

#     success_dim = loader.load_to_dimension_tables(data)
#     if not success_dim:
#         sys.exit(1)

#     success_fact = loader.load_to_fact_table(data)
#     if not success_fact:
#         sys.exit(1)

# PS C:\Users\LEGION\Videos\DIBIMBING\Day 10 - Assignment> python .\scripts\etl\load.py