import os
import pandas as pd
import numpy as np
import time
import multiprocessing
import concurrent.futures
from browser_manager import WebDriverManager
from gofood_navigator import GoFoodNavigator
from gofood_scraper import GoFoodScraper
from scraping_orchestrator import ScrapingOrchestrator

def process_worker(args) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """
    Worker function to be executed in a separate process.

    Each process initializes a `ScrapingOrchestrator` to scrape a chunk of restaurants.
    This function runs scraping using multithreaded scraping inside a multiprocessing worker.

    Parameters:
        args (tuple): A tuple containing:
            - restaurant_chunk (list): The subset of restaurants assigned to this process.
            - existing_review_ids (set): Set of unique review identifiers to prevent duplication.

    Returns:
        tuple:
            - df_d (pd.DataFrame): DataFrame containing restaurant details.
            - df_r (pd.DataFrame): DataFrame containing customer reviews.
            - df_m (pd.DataFrame): DataFrame containing menu items.
            - df_p (pd.DataFrame): DataFrame containing promotions.
            - timings (dict): Execution time spent on each scraping task per process.
    """
    restaurant_chunk, existing_review_ids = args
    process_id = os.getpid()
    print(f"⚙️ Process [ID: {process_id}] started work for {len(restaurant_chunk)} restaurants...")
    
    orchestrator = ScrapingOrchestrator(
        restaurant_list=restaurant_chunk,
        existing_review_ids=existing_review_ids,
        max_workers=5
    )
    df_d, df_r, df_m, df_p, timings = orchestrator.run()
    
    print(f"✅ Process [ID: {process_id}] completed.")
    return df_d, df_r, df_m, df_p, timings


def main() -> None:
    """
    Main function to run the GoFood scraping pipeline in parallel using both multithreading and multiprocessing.

    The workflow includes:
        1. Initializing a headless Selenium WebDriver to navigate and collect the list of nearby restaurants.
        2. Loading existing review data to prevent duplicate scraping.
        3. Splitting the restaurant list into chunks for parallel processing (process-level parallelism).
        4. Launching multiple processes, where each process uses a `ScrapingOrchestrator`
           to scrape restaurant details, menus, promos, and reviews concurrently (thread-level parallelism).
        5. Aggregating the scraped data from all processes into final Pandas DataFrames.
        6. Saving results into CSV files.
        7. Printing a detailed timing report for each scraping task.

    Output Files:
        - data/hasil_detail_restoran.csv
        - data/hasil_ulasan_pelanggan.csv
        - data/hasil_menu_restoran.csv
        - data/hasil_promo_restoran.csv

    Timing Report:
        Prints detailed statistics of scraping time per task (details, menu, promo, reviews).

    Raises:
        Exception: Handles WebDriver failures, file read errors, and multiprocessing exceptions gracefully.
    """
    total_start_time = time.perf_counter()
    
    driver = None
    web_driver_manager = WebDriverManager()
    try:
        driver = web_driver_manager.setup_driver(headless=True)
        if not driver: return

        navigator = GoFoodNavigator(driver)
        navigator.initial_navigation("https://gofood.co.id/id")
        navigator.scroll_and_load_all_data()
        
        scraper = GoFoodScraper()
        initial_restaurant_list = scraper.scrape_restaurant_list(driver)
        
        if not initial_restaurant_list:
            print("No restaurants found.")
            return
            
    finally:
        if driver:
            print("\n🚪 The main driver closes, starting the parallel phase...")
            driver.quit()

    existing_review_ids = set()
    try:
        df_existing_reviews = pd.read_csv('data/hasil_ulasan_pelanggan.csv')
        
        if not df_existing_reviews.empty:
            for _, row in df_existing_reviews.iterrows():
                unique_id = f"{row['Nama Restoran']}_{row['Nama']}_{row['Tanggal Beli']}_{row['Ulasan']}"
                existing_review_ids.add(unique_id)
            print(f"🔍 Found {len(existing_review_ids)} existing reviews.")
        else:
            print("- Review file found but empty, will start scraping from the beginning.")

    except (FileNotFoundError, pd.errors.EmptyDataError):
        print("- If the old review file does not exist or is empty, scraping will start from the beginning.")
        
    num_processes = 3
    print(f"\n🚀 Start scraping with {num_processes} processes (kitchen), each with 5 threads (assistants)...")

    restaurant_chunks = np.array_split(initial_restaurant_list, num_processes)
    
    worker_args = [(chunk, existing_review_ids) for chunk in restaurant_chunks]

    all_results = []
    detail_scraping_start_time = time.perf_counter()

    with concurrent.futures.ProcessPoolExecutor(max_workers=num_processes) as executor:
        all_results = list(executor.map(process_worker, worker_args))
    
    detail_scraping_duration = time.perf_counter() - detail_scraping_start_time

    print("\n🔄 Combining the results of all processes...")
    
    all_df_details = [res[0] for res in all_results]
    all_df_reviews = [res[1] for res in all_results]
    all_df_menus = [res[2] for res in all_results]
    all_df_promos = [res[3] for res in all_results]
    all_timings = [res[4] for res in all_results]

    final_df_details = pd.concat(all_df_details, ignore_index=True)
    final_df_reviews = pd.concat(all_df_reviews, ignore_index=True)
    final_df_menu = pd.concat(all_df_menus, ignore_index=True)
    final_df_promos = pd.concat(all_df_promos, ignore_index=True)

    final_total_timings = {'details': 0, 'menu': 0, 'promo': 0, 'reviews': 0}
    for timings in all_timings:
        for key in final_total_timings:
            final_total_timings[key] += timings[key]

    output_dir = "data" 
    os.makedirs(output_dir, exist_ok=True)
    final_df_details.to_csv(os.path.join(output_dir, 'hasil_detail_restoran.csv'), index=False)
    final_df_reviews.to_csv(os.path.join(output_dir, 'hasil_ulasan_pelanggan.csv'), index=False)
    final_df_menu.to_csv(os.path.join(output_dir, 'hasil_menu_restoran.csv'), index=False)
    final_df_promos.to_csv(os.path.join(output_dir, 'hasil_promo_restoran.csv'), index=False)

    print("\n" + "="*50)
    print("📊 Scraping Process Time Report")
    num_restaurants = len(initial_restaurant_list)
    if num_restaurants > 0:
        print(f"Total Restaurants Processed: {num_restaurants}")
        print(f"⏱️ Real-time Scraping Phase Details: {detail_scraping_duration:.2f} seconds\n")
        
        print("--- Business Details per Task ---")
        
        # Detail Restoran
        total_d = final_total_timings['details']
        avg_d = total_d / num_restaurants
        print(f"🏨 Detail Restoran: Total {total_d:.2f} detik (rata-rata {avg_d:.2f} dtk/resto)")
        
        # Menu
        total_m = final_total_timings['menu']
        avg_m = total_m / num_restaurants
        print(f"📜 Menu:            Total {total_m:.2f} seconds (average {avg_m:.2f} sec/resto)")

        # Promo
        total_p = final_total_timings['promo']
        avg_p = total_p / num_restaurants
        print(f"🎟️ Promotion:           Total {total_p:.2f} seconds (average {avg_p:.2f} seconds/restaurant))")

        # Ulasan
        total_r = final_total_timings['reviews']
        avg_r = total_r / num_restaurants
        print(f"⭐ Review:          Total {total_r:.2f} seconds (average {avg_r:.2f} sec/resto)")
    
    total_duration_seconds = time.perf_counter() - total_start_time
    minutes = int(total_duration_seconds // 60)
    seconds = int(total_duration_seconds % 60)
    print("\n" + "="*50)
    print(f"🎉 The entire process is complete!")
    print(f"⏱️ Total time required: {minutes} minutes {seconds} seconds.")
    print("="*50)

if __name__ == "__main__":
    """
    Entry point of the script.

    Ensures compatibility with Windows by calling `multiprocessing.freeze_support()`,
    which is necessary when using `ProcessPoolExecutor` or `multiprocessing` in scripts
    that may be packaged into executables or run in certain environments.

    Then calls the `main` function to start the scraping pipeline.
    """
    # Di Windows, multiprocessing perlu 'freeze_support'
    # untuk mencegah error saat script di-bundle menjadi executable
    multiprocessing.freeze_support()
    main()