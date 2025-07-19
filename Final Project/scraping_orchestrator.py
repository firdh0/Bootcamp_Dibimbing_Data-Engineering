import concurrent.futures
import pandas as pd
from gofood_scraper import GoFoodScraper

class ScrapingOrchestrator:
    """
    ScrapingOrchestrator is a controller class that manages concurrent scraping
    of GoFood restaurant data using multithreading.

    This class distributes restaurant scraping tasks across multiple threads,
    collects the results, and aggregates data into Pandas DataFrames.

    Attributes:
        restaurant_list (list): List of restaurants to scrape, each represented as a dictionary.
        existing_review_ids (set): Set of unique review identifiers to avoid duplicate scraping.
        max_workers (int): Number of concurrent threads to use (default is 5).
        scraper (GoFoodScraper): Instance of the GoFoodScraper class to handle the actual scraping logic.

    Methods:
        run() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
            Executes the multithreaded scraping workflow, returning aggregated results and timings.
    """


    def __init__(self, restaurant_list: list, existing_review_ids: set, max_workers: int = 5) -> None:
        """
        Initializes the ScrapingOrchestrator with required data and configurations.

        Parameters:
            restaurant_list (list): The list of restaurants to scrape. Each element is a dictionary
                                    containing at least 'Nama Restoran' and 'Link'.
            existing_review_ids (set): A set of unique review IDs to avoid re-scraping existing reviews.
            max_workers (int, optional): Maximum number of concurrent threads. Defaults to 5.
        """
        self.restaurant_list = restaurant_list
        self.existing_review_ids = existing_review_ids
        self.max_workers = max_workers
        self.scraper = GoFoodScraper()


    def run(self) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
        """
        Executes the scraping process concurrently using ThreadPoolExecutor.

        For each restaurant, the method performs:
            - Scraping of restaurant details
            - Scraping of menu items
            - Scraping of promos
            - Scraping of user reviews

        Results from each thread are aggregated into Pandas DataFrames.

        Returns:
            tuple:
                - df_restaurants (pd.DataFrame): DataFrame containing restaurant details.
                - df_reviews (pd.DataFrame): DataFrame containing user reviews.
                - df_menu (pd.DataFrame): DataFrame containing menu items.
                - df_promos (pd.DataFrame): DataFrame containing promotional offers.
                - total_timings (dict): Dictionary summarizing total execution time for each task type
                                       ('details', 'menu', 'promo', 'reviews').

        Raises:
            Exception: Captures and logs exceptions raised by individual scraping threads,
                       allowing other threads to continue running.
        """
        detailed_results = []
        all_reviews_results = []
        all_menu_results = []
        all_promos_results = []

        total_timings = {'details': 0, 'menu': 0, 'promo': 0, 'reviews': 0}
        
        scraper = GoFoodScraper()

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.scraper.get_single_restaurant_details, restaurant, index + 1, len(self.restaurant_list), self.existing_review_ids): restaurant
                for index, restaurant in enumerate(self.restaurant_list)
            }
            
            for future in concurrent.futures.as_completed(futures):
                restaurant_data = futures[future]
                try:
                    restaurant_details, menu_items, promo_items, reviews, timings = future.result()

                    if restaurant_details: detailed_results.extend(restaurant_details)
                    if reviews: all_reviews_results.extend(reviews)
                    if menu_items: all_menu_results.extend(menu_items)
                    if promo_items: all_promos_results.extend(promo_items)

                    for key in total_timings:
                        total_timings[key] += timings[key]

                except Exception as e:
                    restaurant_name = restaurant_data['Nama Restoran']
                    print(f"❌ An exception was raised by the thread for '{restaurant_name}': {e}")

        df_restaurants = pd.DataFrame(detailed_results) if detailed_results else pd.DataFrame()
        df_reviews = pd.DataFrame(all_reviews_results) if all_reviews_results else pd.DataFrame()
        df_menu = pd.DataFrame(all_menu_results) if all_menu_results else pd.DataFrame()
        df_promos = pd.DataFrame(all_promos_results) if all_promos_results else pd.DataFrame()

        return df_restaurants, df_reviews, df_menu, df_promos, total_timings