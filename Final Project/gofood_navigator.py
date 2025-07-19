import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup

class GoFoodNavigator:
    """
    GoFoodNavigator is a helper class designed to automate the navigation and data extraction
    process from the GoFood web platform using Selenium WebDriver.

    This class allows automated interactions such as selecting the user's current location,
    clicking on specific categories, performing dynamic scrolling to load all data, and extracting
    restaurant distance information from the loaded HTML content.

    Attributes:
        driver (WebDriver): The Selenium WebDriver instance used for browser automation.

    Methods:
        initial_navigation(url: str) -> None:
            Navigates to the specified GoFood URL, selects current location, and clicks the 'Terdekat' category.

        get_all_distances_from_html() -> list[float]:
            Extracts all restaurant distances (in km) from the currently loaded page.

        scroll_and_load_all_data() -> None:
            Performs dynamic scrolling and clicks 'Muat lebih banyak' buttons until all data is loaded.
    """


    def __init__(self, driver: WebDriver) -> None:
        """
        Initializes the GoFoodNavigator with a Selenium WebDriver.

        Parameters:
            driver (WebDriver): The Selenium WebDriver instance to control the browser.
        """
        self.driver = driver


    def initial_navigation(self, url):
        """
        Performs the initial steps to open GoFood's web interface, select the current location,
        and navigate to the 'Terdekat' (Nearby) category.

        Steps performed:
            - Open the provided URL.
            - Click the location input field.
            - Select 'Pakai lokasimu saat ini' (Use your current location).
            - Click the 'Eksplor' (Explore) button.
            - Click the 'Terdekat' category.

        Parameters:
            url (str): The URL of GoFood's website or specific landing page to open.

        Raises:
            TimeoutException: If any of the clickable elements cannot be found within the wait time.
        """
        print(f"\n🌍 Opening URL: {url}")
        self.driver.get(url)

        print("🔎 Finding and clicking the location input field...")
        location_input = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.ID, "location-picker"))
        )
        location_input.click()
        print("👍 Location input field clicked.")

        print("📍 Clicking 'Use your current location' button...")
        use_current_location_button = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '//*[contains(text(), "Pakai lokasimu saat ini")]'))
        )
        use_current_location_button.click()
        print("👍 'Use your current location' button clicked.")

        print("🧭 Clicking the 'Explore' button...")
        explore_button = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '//button[contains(., "Eksplor")]'))
        )
        explore_button.click()
        print("👍 'Explore' button clicked.")
        
        time.sleep(3) 

        print("🏠 Finding and clicking the 'Terdekat' category...")
        terdekat_category_button = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, "//h3[@title='Terdekat']"))
        )
        terdekat_category_button.click()
        print("👍 'Terdekat' category successfully clicked.")


    def get_all_distances_from_html(self) -> list[float]:
        """
        Extracts all restaurant distance information from the current HTML page source.

        The method looks for anchor tags linking to restaurant pages and extracts the
        distance text (in kilometers) from spans with class 'gf-label-s'.

        Returns:
            list[float]: A list of distances (in km) for each restaurant found.

        Notes:
            If a distance cannot be converted to float, it will be skipped.
        """
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        cards = soup.select('a[href*="/restaurant/"]')
        all_distances = []

        for card in cards:
            span = card.find("span", class_="gf-label-s")
            if span and 'km' in span.text:
                try:
                    dist = float(span.text.replace(' km', '').strip())
                    all_distances.append(dist)
                except ValueError:
                    continue

        print(f"  ➕ Total found {len(all_distances)} distances of restaurants from the page.")
        return all_distances


    def scroll_and_load_all_data(self) -> None:
        """
        Dynamically scrolls the GoFood page and loads all restaurant data by clicking
        the 'Muat lebih banyak' (Load more) button when available.

        The method performs multiple scrolls, and after each scroll, it checks for the
        'Muat lebih banyak' button. If the button is found, it clicks it and resets the
        scroll attempt counter. If not found repeatedly, the method assumes all data is loaded.

        Returns:
            None

        Raises:
            TimeoutException: If the 'Muat lebih banyak' button is not found within the wait time.
        """
        print("\n🔄 Starting dynamic scroll to load all restaurants...")
        scroll_attempts = 0
        max_scroll_attempts_without_button = 1

        while scroll_attempts < max_scroll_attempts_without_button:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            print("   Scrolling down...")
            time.sleep(0.2)

            try:
                load_more_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, '//button[.//span[text()="Muat lebih banyak"]]'))
                )
                load_more_button.click()
                print("   👍 'Muat lebih banyak' button found and clicked.")
                scroll_attempts = 0
            except TimeoutException:
                scroll_attempts += 1
                print(f"   ⏳ 'Muat lebih banyak' button not found (Attempt {scroll_attempts}/{max_scroll_attempts_without_button}).")

        print("\n✅ Scrolling finished. Assuming all data is now loaded.")