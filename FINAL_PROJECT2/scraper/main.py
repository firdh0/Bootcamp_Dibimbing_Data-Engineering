import os
import pandas as pd
import numpy as np
import time
import multiprocessing
import concurrent.futures
import argparse

from browser_manager import WebDriverManager
from gofood_navigator import GoFoodNavigator
from gofood_scraper import GoFoodScraper
from scraping_orchestrator import ScrapingOrchestrator
from gcs_uploader import GCSUploader 
from datastore_manager import DatastoreManager
from datetime import datetime

DEFAULT_GCS_BUCKET = "gofood-data-lake-bucket"
DEFAULT_GCS_PATH_PREFIX = "bronze/raw"
DEFAULT_NUM_PROCESSES = 3

class GoFoodScrapingPipeline:
    """
    A pipeline for scraping GoFood restaurant data, reviews, menus, and promotions 
    using multiprocessing, and uploading the results to Google Cloud Storage (GCS).

    This pipeline handles the following tasks:
    - Navigating the GoFood website to collect restaurant links.
    - Scraping detailed information including reviews, menu, and promo data.
    - Using parallel processing to speed up scraping across multiple restaurants.
    - Uploading the results directly to a specified GCS bucket in CSV format.

    Attributes:
        gcs_bucket (str): The name of the GCS bucket where data will be uploaded.
        gcs_path_prefix (str): The folder or prefix path in GCS where files will be saved.
        num_processes (int): Number of parallel processes for scraping.
        web_driver_manager (WebDriverManager): Manager for Selenium WebDriver lifecycle.
        gcs_uploader (GCSUploader): Utility for uploading Pandas DataFrames to GCS.

    Methods:
        _process_worker(args) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
            Worker function executed in parallel for a chunk of restaurants.
        
        _get_restaurant_list() -> list:
            Collects the list of restaurant links by automating GoFood navigation.

        run() -> None:
            Executes the entire pipeline: scraping, parallel processing, and GCS upload.
            
    Raises:
        Exception: If GCS bucket is not accessible or scraping encounters critical errors.
    """


    def __init__(self, gcs_bucket: str = DEFAULT_GCS_BUCKET, gcs_path_prefix: str = DEFAULT_GCS_PATH_PREFIX, num_processes: int = DEFAULT_NUM_PROCESSES) -> None:
        """
        Initializes the GoFoodScrapingPipeline with GCS configuration and scraping parameters.

        Parameters:
            gcs_bucket (str): The target GCS bucket. Defaults to DEFAULT_GCS_BUCKET.
            gcs_path_prefix (str): Path prefix/folder inside the GCS bucket. Defaults to DEFAULT_GCS_PATH_PREFIX.
            num_processes (int): Number of processes to run in parallel. Defaults to DEFAULT_NUM_PROCESSES.
        """
        self.gcs_bucket = gcs_bucket
        self.gcs_path_prefix = gcs_path_prefix
        self.num_processes = num_processes
        self.web_driver_manager = WebDriverManager()
        self.gcs_uploader = GCSUploader(bucket_name=self.gcs_bucket)


    @staticmethod
    def _process_worker(args) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
        """
        Worker function for scraping data in parallel.

        Each worker:
        - Initializes its own Datastore connection.
        - Uses ScrapingOrchestrator to scrape restaurant details, reviews, menu, and promos.

        Parameters:
            args (tuple): 
                - restaurant_chunk (list): Subset of restaurants assigned to the process.
                - datastore_manager_instance (DatastoreManager or project_id placeholder): Datastore manager instance or project ID.

        Returns:
            tuple: 
                - df_d (pd.DataFrame): Restaurant detail DataFrame.
                - df_r (pd.DataFrame): Review DataFrame.
                - df_m (pd.DataFrame): Menu DataFrame.
                - df_p (pd.DataFrame): Promo DataFrame.
                - timings (dict): Timing information for monitoring.
        """
        restaurant_chunk, datastore_manager_instance = args 
        process_id = os.getpid()
        print(f"⚙️ Process [ID: {process_id}] started work for {len(restaurant_chunk)} restaurants...")
        
        datastore_manager_instance = DatastoreManager(project_id='gofood-465817')
        orchestrator = ScrapingOrchestrator(
            restaurant_list=restaurant_chunk,
            datastore_manager=datastore_manager_instance,
            max_workers=5
        )
        df_d, df_r, df_m, df_p, timings = orchestrator.run()
        
        print(f"✅ Process [ID: {process_id}] completed.")
        return df_d, df_r, df_m, df_p, timings


    def _get_restaurant_list(self) -> list:
        """
        Retrieves the list of GoFood restaurant links to be scraped.

        Uses a Selenium WebDriver to:
        - Open GoFood website.
        - Perform scrolling to load all restaurants.
        - Extract restaurant links.

        Returns:
            list: A list of restaurant link dictionaries to be processed.
        """
        driver = None
        try:
            driver = self.web_driver_manager.setup_driver(headless=True)
            if not driver: return []

            navigator = GoFoodNavigator(driver)
            navigator.initial_navigation("https://gofood.co.id/id")
            navigator.scroll_and_load_all_data()
            
            scraper = GoFoodScraper()
            return scraper.scrape_restaurant_list(driver)
        finally:
            if driver:
                print("\n🚪 Main driver closed, starting parallel phase...")
                driver.quit()


    def run(self):
        """
        Runs the complete GoFood scraping pipeline.

        Workflow:
        1. Collect restaurant list.
        2. Split list into chunks for parallel processing.
        3. Launch multiple processes using ProcessPoolExecutor.
        4. Combine results from all workers.
        5. Upload combined dataframes (restaurant details, reviews, menus, promos) to GCS.

        Upload Rules:
        - If there are no new reviews, an empty CSV (with headers) is still uploaded 
          to keep folder structure consistent in GCS.

        Logs:
            Prints detailed progress and time tracking information.
        """
        total_start_time = time.perf_counter()

        initial_restaurant_list = self._get_restaurant_list()
        if not initial_restaurant_list:
            print("No restaurants found. Process stopped.")
            return
        
        print("- Start scraping from the beginning for this batch.")

        print(f"\n🚀 Starting scraping with {self.num_processes} processes...")
        restaurant_chunks = np.array_split(initial_restaurant_list, self.num_processes)
        worker_args = [(chunk, 'gofood-465817') for chunk in restaurant_chunks] 

        all_results = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=self.num_processes) as executor:
            all_results = list(executor.map(GoFoodScrapingPipeline._process_worker, worker_args))

        print("\n🔄 Combining the results of all processes...")
        final_df_details = pd.concat([res[0] for res in all_results], ignore_index=True)
        new_df_reviews_from_scrape = pd.concat([res[1] for res in all_results], ignore_index=True)
        final_df_menu = pd.concat([res[2] for res in all_results], ignore_index=True)
        final_df_promos = pd.concat([res[3] for res in all_results], ignore_index=True)

        print("\n" + "="*50)
        print(f"☁️ Upload results to GCS Bucket: {self.gcs_bucket}")
        
        self.gcs_uploader.upload_df(final_df_details, f"{self.gcs_path_prefix}/hasil_detail_restoran.csv")
        timestamp_str = datetime.now().strftime('%Y%m%d%H%M%S')

        reviews_blob_name = f"{self.gcs_path_prefix}/ulasan_pelanggan/part-{timestamp_str}.csv"
        empty_reviews_blob_name = f"{self.gcs_path_prefix}/ulasan_pelanggan/empty-part-{timestamp_str}.csv" # Ini sudah benar

        if not new_df_reviews_from_scrape.empty:
            self.gcs_uploader.upload_df(new_df_reviews_from_scrape, reviews_blob_name)
            print(f"✅ Successfully uploaded {len(new_df_reviews_from_scrape)} new reviews to {reviews_blob_name}")
        else:
            expected_review_cols = [
                'Nama Restoran', 'Nama', 'Pengguna Gojek Sejak', 'Rating', 'Ulasan',
                'Produk yang Dibeli', 'Tanggal Beli', 'Waktu Scraping'
            ]
            empty_reviews_df = pd.DataFrame(columns=expected_review_cols)
            
            self.gcs_uploader.upload_df(empty_reviews_df, empty_reviews_blob_name)
            print(f"✅ No new reviews found. Upload an empty file (just the header) to {empty_reviews_blob_name} to keep the folder structure.")
        self.gcs_uploader.upload_df(final_df_menu, f"{self.gcs_path_prefix}/hasil_menu_restoran.csv")
        self.gcs_uploader.upload_df(final_df_promos, f"{self.gcs_path_prefix}/hasil_promo_restoran.csv")

        total_duration_seconds = time.perf_counter() - total_start_time
        minutes = int(total_duration_seconds // 60)
        seconds = int(total_duration_seconds % 60)
        print("\n" + "="*50)
        print(f"🎉 The entire process is complete!")
        print(f"⏱️ Total time required: {minutes} minutes {seconds} seconds.")
        print("="*50)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GoFood Scraper with GCS Upload")
    parser.add_argument("--gcs-bucket", default=DEFAULT_GCS_BUCKET, help=f"Nama GCS Bucket tujuan. (Default: {DEFAULT_GCS_BUCKET})")
    parser.add_argument("--gcs-path-prefix", default=DEFAULT_GCS_PATH_PREFIX, help=f"Prefix path/folder di dalam GCS bucket. (Default: {DEFAULT_GCS_PATH_PREFIX})")
    args = parser.parse_args()

    multiprocessing.freeze_support()
    
    pipeline = GoFoodScrapingPipeline(
        gcs_bucket=args.gcs_bucket,
        gcs_path_prefix=args.gcs_path_prefix
    )
    pipeline.run()
