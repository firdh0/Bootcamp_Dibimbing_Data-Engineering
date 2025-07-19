import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException, NoSuchElementException

from browser_manager import WebDriverManager
from datastore_manager import DatastoreManager

class GoFoodScraper:
    """
    GoFoodScraper is a web scraping utility for extracting detailed information 
    from GoFood restaurant pages using Selenium and BeautifulSoup.

    This class handles scraping multiple aspects of GoFood pages, including:
    - Restaurant list and basic info
    - Restaurant menus
    - Promotions (promos)
    - User reviews
    - Detailed restaurant information (rating, price, address, hours)

    Methods:
        scrape_restaurant_list(driver) -> list[dict]:
            Scrapes the list of restaurants from the current page.

        scrape_reviews(driver, restaurant_name, existing_review_ids) -> list[dict]:
            Scrapes user reviews from the restaurant's review section.

        scrape_menu(driver, restaurant_name) -> list[dict]:
            Scrapes menu items from a restaurant's detail page.

        scrape_promos(driver, restaurant_name) -> list[dict]:
            Scrapes promotions available for a restaurant.

        get_single_restaurant_details(restaurant, index, total, existing_review_ids) -> tuple:
            Scrapes detailed information, menu, promos, and reviews of a single restaurant in a separate browser instance.
    """


    def __init__(self) -> None:
        """
        Initializes the GoFoodScraper instance.

        No specific configuration is needed during initialization.
        """
        pass


    def scrape_restaurant_list(self, driver: WebDriver) -> list[dict]:
        """
        Scrapes the initial list of restaurants from the GoFood restaurant listing page.

        Parameters:
            driver (WebDriver): An active Selenium WebDriver instance.

        Returns:
            list[dict]: A list of dictionaries containing 'Nama Restoran' and 'Link' for each restaurant.
        """
        print("\n🍽️ Waiting for the restaurant list to be fully present...")
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'my-6')]"))
        )
        print("👍 Restaurant list is present.")
        
        print("📄 Getting and parsing page source code...")
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')

        scraped_data = []
        restaurant_container = soup.find('div', class_=lambda c: c and 'my-6' in c.split())

        if not restaurant_container:
            print("❌ Could not find the main restaurant container on the page.")
            return []
        
        restaurant_cards = restaurant_container.find_all('a', recursive=False)
        print(f"✅ Found {len(restaurant_cards)} restaurants. Processing initial data...")

        for card in restaurant_cards:
            name_tag = card.find('p', class_='gf-label-m')
            name = name_tag['title'].strip() if name_tag and name_tag.has_attr('title') else 'N/A'
            
            link_href = card.get('href', '')
            full_link = f"https://gofood.co.id{link_href}" if link_href and link_href.startswith('/') else link_href
            
            if name != 'N/A' and full_link:
                scraped_data.append({
                    'Nama Restoran': name,
                    'Link': full_link
                })
                
        return scraped_data


    def scrape_reviews(self, driver: WebDriver, restaurant_name: str, last_scraped_timestamp_for_this_restaurant: datetime) -> tuple[list[dict], datetime]:
        """
        Scrapes customer reviews for a specific restaurant, ensuring no duplication 
        based on unique review identifiers.

        Parameters:
            driver (WebDriver): An active Selenium WebDriver instance.
            restaurant_name (str): The name of the restaurant being scraped.
            existing_review_ids (set): A set of previously scraped review IDs to prevent duplicates.

        Returns:
            list[dict]: A list of dictionaries containing review details such as name, rating, 
                        product bought, review text, date, and user info.
        """
        print(f"   - Scraping reviews for '{restaurant_name}' with boundary: {last_scraped_timestamp_for_this_restaurant}...")
        all_reviews_data = []
        
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'flex flex-col space-y-10')]"))
            )
        except TimeoutException:
            print("   - ⚠️ No review container found on the page.")
            return [], datetime.min

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        review_container = soup.find('div', class_=lambda c: c and 'flex' in c and 'flex-col' in c and 'space-y-10' in c)
        
        if not review_container:
            print("   - ⚠️ Could not find review container.")
            return [], datetime.min

        review_cards = review_container.find_all('div', recursive=False)
        print(f"   - Found {len(review_cards)} reviews to parse.")
        
        new_reviews_found_count = 0
        latest_review_date_this_scrape = datetime.min
        for card in review_cards:
            try:
                name_tag = card.find('h3', class_='text-gf-content-secondary gf-label-m')
                name = name_tag.text.strip() if name_tag else 'N/A'

                review_text_tag = card.find('p', class_='break-words gf-body-m')
                review_text = review_text_tag.text.strip() if review_text_tag else ''

                bought_date_tag = card.find('div', class_='mt-4 text-gf-content-muted gf-body-s')
                bought_date = bought_date_tag.text.replace('Dibeli tanggal', '').strip() if bought_date_tag else 'N/A'

                if bought_date.startswith("Dibeli tanggal "):
                    bought_date_str = bought_date.replace("Dibeli tanggal ", "").strip()
                else:
                    bought_date_str = bought_date 

                review_date_parsed = self._parse_review_date(bought_date_str) 

                if review_date_parsed is None:
                    print(f"   - ⚠️ Failed to parse review date: ‘{bought_date_str}’ for ‘{restaurant_name}’. Review ignored.")
                    continue

                if review_date_parsed <= last_scraped_timestamp_for_this_restaurant:
                    print(f"   🛑 Stopping scraping reviews for ‘{restaurant_name}’ because it reached old reviews (Date: {review_date_parsed}).")
                    break

                new_reviews_found_count += 1 

                since_tag = card.find('span', class_='mt-1 text-gf-content-muted gf-body-xs md:gf-body-s')
                since = since_tag.text.replace('Pengguna Gojek sejak', '').strip() if since_tag else 'N/A'

                rating_tag = card.find('span', class_='ml-1 inline-block')
                rating = rating_tag.text.strip() if rating_tag else 'N/A'
                
                product_tag = card.find('span', class_='ml-2 break-words md:mt-1')
                products = product_tag.text.strip() if product_tag else 'N/A'

                cleaned_review_text = review_text.encode('utf-8', 'ignore').decode('utf-8')
                all_reviews_data.append({
                    'Nama Restoran': restaurant_name,
                    'Nama': name,
                    'Pengguna Gojek Sejak': since,
                    'Rating': rating,
                    'Ulasan': cleaned_review_text,
                    'Produk yang Dibeli': products,
                    'Tanggal Beli': bought_date,
                    'Waktu Scraping': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })

                if review_date_parsed > latest_review_date_this_scrape:
                    latest_review_date_this_scrape = review_date_parsed
            except Exception as e:
                print(f"   - ❗️ Error parsing a review card: {e}")
                continue
        
        print(f"   - ✅ {new_reviews_found_count} new reviews found for '{restaurant_name}'.")
        return all_reviews_data, latest_review_date_this_scrape
    
    
    def _parse_review_date(self, date_str: str) -> datetime | None:
        """
        Parses a review date string from Indonesian formats into a Python datetime object.

        This method handles both **relative date expressions** (e.g., "2 hari yang lalu", "3 jam yang lalu") 
        and **absolute date formats** commonly found in Indonesian user interfaces (e.g., "12 Januari 2024", "5/7/2024").

        Supported parsing includes:
        - Relative time: "X hari yang lalu", "X jam yang lalu", "X menit yang lalu", "X detik yang lalu".
        - Absolute dates with Indonesian month names (both full and abbreviated).
        - Common date formats: `"%d %b %Y"`, `"%d %b"`, `"%d/%m/%Y"`, `"%d/%m/%y"`.
        
        Parameters:
            date_str (str): The date string extracted from the review metadata, in Indonesian language.

        Returns:
            datetime | None: 
                - A timezone-naive `datetime` object representing the parsed date.
                - Returns `None` if parsing fails.

        Examples:
            >>> _parse_review_date("2 hari yang lalu")
            datetime.datetime(2025, 7, 17, ...)

            >>> _parse_review_date("5 Juli 2024")
            datetime.datetime(2024, 7, 5, 0, 0)

        Notes:
            - If the year is missing in the date string, the current year is assumed.
            - Month translation is handled automatically from Indonesian to English abbreviations 
            to comply with `strptime` requirements.
        
        Raises:
            None explicitly, but silently continues to try different formats if parsing fails.
        """
        date_str = date_str.lower().strip()
        now = datetime.now()

        if 'hari yang lalu' in date_str:
            try:
                days_ago = int(date_str.split(' ')[0])
                return now - timedelta(days=days_ago)
            except ValueError:
                pass
        elif 'jam yang lalu' in date_str:
            try:
                hours_ago = int(date_str.split(' ')[0])
                return now - timedelta(hours=hours_ago)
            except ValueError:
                pass
        elif 'menit yang lalu' in date_str:
            try:
                minutes_ago = int(date_str.split(' ')[0])
                return now - timedelta(minutes=minutes_ago)
            except ValueError:
                pass
        elif 'detik yang lalu' in date_str:
            try:
                seconds_ago = int(date_str.split(' ')[0])
                return now - timedelta(seconds=seconds_ago)
            except ValueError:
                pass
        else:
            import re
            
            bulan_map_full = {
                'januari': 'jan', 'februari': 'feb', 'maret': 'mar', 'april': 'apr',
                'mei': 'may', 'juni': 'jun', 'juli': 'jul', 'agustus': 'aug',
                'september': 'sep', 'oktober': 'oct', 'november': 'nov', 'desember': 'dec'
            }
            bulan_map_abbr = {
                'jan': 'jan', 'feb': 'feb', 'mar': 'mar', 'apr': 'apr', 'mei': 'may', 'jun': 'jun',
                'jul': 'jul', 'agu': 'aug', 'sep': 'sep', 'okt': 'oct', 'nov': 'nov', 'des': 'dec'
            }

            temp_date_str = date_str
            for indo_month_full, eng_month_abbr in bulan_map_full.items():
                temp_date_str = re.sub(r'\b' + indo_month_full + r'\b', eng_month_abbr, temp_date_str)
            
            for indo_month_abbr, eng_month_abbr in bulan_map_abbr.items():
                 temp_date_str = re.sub(r'\b' + indo_month_abbr + r'\b', eng_month_abbr, temp_date_str)

            formats = [
                "%d %b %Y",       
                "%d %b",          
                "%d/%m/%Y",       
                "%d/%m/%y"        
            ]
            
            for fmt in formats:
                try:
                    parsed_date = datetime.strptime(temp_date_str, fmt)
                    if '%Y' not in fmt and '%y' not in fmt:
                        parsed_date = parsed_date.replace(year=now.year)
                    return parsed_date.replace(tzinfo=None)
                except ValueError:
                    continue
        return None
    

    def scrape_menu(self, driver, restaurant_name) -> list[dict]:
        """
        Scrapes the menu items from a restaurant's detail page.

        Parameters:
            driver (WebDriver): An active Selenium WebDriver instance.
            restaurant_name (str): The name of the restaurant whose menu is being scraped.

        Returns:
            list[dict]: A list of dictionaries containing menu item details such as name, 
                        price, availability status, and description.
        """
        print("   - 📜 Scraping menu...")
        all_menu_data = []
        
        try:
            page_soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            potential_cards = page_soup.select('div.mt-4.md\\:mx-2, div.items-stretch.justify-between, a[href*="/gofood/pesan"]')
            print(f"   - Found {len(potential_cards)} potential menu cards to analyze.")

            for card in potential_cards:
                name_tag = card.find('h3', class_='text-gf-content-primary')
                if not name_tag:
                    continue 

                name = name_tag.get_text(strip=True)
                
                detail_tag = card.find('p', class_='text-gf-content-muted')
                detail = detail_tag.get_text(strip=True) if detail_tag else 'N/A'
                
                price = 'N/A'
                price_spans = card.find_all('span')
                for span in price_spans:
                    potential_price = span.get_text(strip=True).replace('.', '').replace('Rp', '').strip()
                    if potential_price.isdigit():
                        price = span.get_text(strip=True)
                        break 
                
                status_tag = card.find('span', class_='text-gf-background-fill-brand')
                status = "Tersedia" # Default status
                if status_tag and 'habis' in status_tag.get_text(strip=True).lower():
                    status = "Habis"

                if name and price != 'N/A':
                    all_menu_data.append({
                        'Nama Restoran': restaurant_name,
                        'Nama Menu': name,
                        'Detail Menu': detail,
                        'Harga': price,
                        'Status': status,
                        'Waktu Scraping': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                    
        except Exception as e:
            print(f"   - ❗️ An error occurred while parsing the menu item: {e}")

        print(f"   - ✅ Success scrape {len(all_menu_data)} item menu.")
        return all_menu_data


    def scrape_promos(self, driver, restaurant_name) -> list[dict]:
        """
        Scrapes the available promotions (promos) for a given restaurant.

        Parameters:
            driver (WebDriver): An active Selenium WebDriver instance.
            restaurant_name (str): The name of the restaurant whose promos are being scraped.

        Returns:
            list[dict]: A list of dictionaries containing promo title, usage instructions, 
                        and details.
        """
        print("   - 🎟️ Searching and extracting promotions...")
        all_promo_data = []

        try:
            lihat_semua_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//div[contains(., 'promo')]//button[.//span[text()='Lihat semua']]"))
            )
            lihat_semua_button.click()
            print("   - The “Lihat semua ” promo button is clicked.")

            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'space-y-3.5')]"))
            )
            time.sleep(1) 

            page_soup = BeautifulSoup(driver.page_source, 'html.parser')
            promo_cards = page_soup.select('div.ineligible > div, div.eligible > div')

            for card in promo_cards:
                title_tag = card.find('div', class_='gf-label-l')
                usage_tag = card.find('div', class_='gf-label-xs')
                
                if title_tag and usage_tag:
                    title = title_tag.get_text(strip=True)
                    usage = usage_tag.get_text(strip=True)
                    
                    detail_items = card.find_all('li', class_='gf-body-s')
                    details = ", ".join([item.get_text(strip=True) for item in detail_items]) if detail_items else 'N/A'

                    all_promo_data.append({
                        'Nama Restoran': restaurant_name,
                        'Judul Promo': title,
                        'Cara Menggunakan': usage,
                        'Detail Promo': details,
                        'Waktu Scraping': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
            
            driver.find_element(By.TAG_NAME, 'body').send_keys(webdriver.common.keys.Keys.ESCAPE)
            print(f"   - ✅ Successfully scraped {len(all_promo_data)} promotions. Pop-up closed.")

        except TimeoutException:
            print("   - ℹ️ There is no ‘View all’ button for the promotions found.")
        except Exception as e:
            print(f"   - ❗️ An error occurred while parsing the promo: {e}")

        return all_promo_data


    def get_single_restaurant_details(self, restaurant, index, total, datastore_manager: DatastoreManager) -> tuple[list[dict], list[dict], list[dict], list[dict], dict]:
        """
        Scrapes complete details of a single restaurant including:
        - General info (rating, distance, cuisine, price level, address, hours)
        - Menu items
        - Promotions
        - User reviews

        Parameters:
            restaurant (dict): A dictionary containing at least 'Nama Restoran' and 'Link'.
            index (int): The index of the current restaurant in the scraping sequence.
            total (int): The total number of restaurants to scrape.
            existing_review_ids (set): A set of previously collected review IDs to avoid duplicates.

        Returns:
            tuple: A tuple containing:
                - restaurant_details (list[dict]): Basic info and meta data of the restaurant.
                - menu_items (list[dict]): The scraped menu items.
                - promo_items (list[dict]): The scraped promotions.
                - reviews (list[dict]): The scraped customer reviews.
                - timings (dict): Time taken for each scraping step.
        """
        driver = None
        web_driver_manager = WebDriverManager()
        restaurant_name = restaurant['Nama Restoran']
        print(f"[{index}/{total}] THREAD START: Scraping '{restaurant_name}'...")
        
        timings = {'details': 0, 'menu': 0, 'promo': 0, 'reviews': 0}

        try:
            start_details_time = time.perf_counter()

            driver = web_driver_manager.setup_driver()
            if not driver:
                raise Exception("Failed to setup driver for this thread.")
            
            driver.get(restaurant['Link'])
            print("   - Loading restaurant detail page...")
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, "//a[text()='Jarak']"))
            )
            time.sleep(2)

            page_soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            rating, distance, price_str, price_detail = 'N/A', 'N/A', 'N/A', 'N/A'
            address, opening_hours, cuisine = "Alamat tidak ditemukan", {}, "N/A"
            
            cuisine_tag = page_soup.find('p', class_="text-gf-content-secondary line-clamp-1 gf-body-s md:gf-body-m lg:gf-body-l")
            if cuisine_tag:
                cuisine = cuisine_tag.text.strip()

            info_panel = page_soup.find('div', class_=lambda c: c and 'mr-12' in c and 'inline-flex' in c)
            if info_panel:
                rating_container = info_panel.find('svg', class_=lambda c: c and 'text-gf-support-warning-default' in c)
                if rating_container:
                    rating_tag = rating_container.find_next_sibling('p')
                    rating = rating_tag.text.strip() if rating_tag else rating
                
                distance_container = info_panel.find('svg', class_=lambda c: c and 'text-gf-brand-retail-red' in c)
                if distance_container:
                    distance_tag = distance_container.find_next_sibling('p')
                    distance = distance_tag.text.strip() if distance_tag else distance
                
                price_level_tag = info_panel.find('div', attrs={'data-testid': 'priceLevel'})
                if price_level_tag:
                    active_dollars = price_level_tag.find_all('div', class_='text-gf-content-primary')
                    price_level = len(active_dollars)
                    price_str = f"{'$' * price_level}{'·' * (4 - price_level)}"
                    price_detail_container = price_level_tag.find_parent('div').find_next_sibling('div')
                    if price_detail_container:
                        price_detail_tag = price_detail_container.find('span')
                        price_detail = price_detail_tag.text.strip() if price_detail_tag else price_detail

            print(f"   - Main details: Rating: {rating}, Distance: {distance}, Price: {price_str}, Detail: {price_detail}")

            print("   - Clicking 'Jarak' button for address...")
            jarak_button = driver.find_element(By.XPATH, "//a[text()='Jarak']")
            jarak_button.click()
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//h2[starts-with(@id, 'headlessui-dialog-title-')]")))
            time.sleep(1)
            
            popup_soup = BeautifulSoup(driver.page_source, 'html.parser')
            address_tag = popup_soup.find('div', class_='text-gf-content-muted gf-body-s')
            address = address_tag.text.strip() if address_tag else address
            hours_container = popup_soup.find('h4', string='Jam buka')
            if hours_container:
                day_elements = hours_container.find_next_siblings('div')
                for day_element in day_elements:
                    day_tag = day_element.find('div', class_=lambda c: c and 'gf-label-s' in c)
                    hour_tag = day_element.find('div', class_='text-left')
                    if day_tag and hour_tag:
                        opening_hours[day_tag.text.strip()] = hour_tag.text.strip()
            
            driver.find_element(By.TAG_NAME, 'body').send_keys(webdriver.common.keys.Keys.ESCAPE)
            time.sleep(1)
            
            print(f"   ✅ Success: Scraped details for {restaurant['Nama Restoran']}")

            start_menu_time = time.perf_counter()
            menu_items = self.scrape_menu(driver, restaurant['Nama Restoran'])
            timings['menu'] = time.perf_counter() - start_menu_time
            if menu_items:
                print(f"   - ✅ Successfully scraped {len(menu_items)} menu items.")

            start_promo_time = time.perf_counter()
            promo_items = self.scrape_promos(driver, restaurant['Nama Restoran'])
            timings['promo'] = time.perf_counter() - start_promo_time
            if promo_items:
                print(f"   - ✅ Successfully scraped {len(promo_items)} promotions.")
            
            restaurant_details = [{
                'Nama Restoran': restaurant['Nama Restoran'], 'Rating': rating, 'Jarak': distance,
                'Tingkat Harga': price_str, 'Detail Harga': price_detail, 'Tipe Masakan': cuisine, 'Alamat': address,
                'Jam Buka (Senin)': opening_hours.get('Senin', 'Tutup'), 'Jam Buka (Selasa)': opening_hours.get('Selasa', 'Tutup'),
                'Jam Buka (Rabu)': opening_hours.get('Rabu', 'Tutup'), 'Jam Buka (Kamis)': opening_hours.get('Kamis', 'Tutup'),
                'Jam Buka (Jumat)': opening_hours.get('Jumat', 'Tutup'), 'Jam Buka (Sabtu)': opening_hours.get('Sabtu', 'Tutup'),
                'Jam Buka (Minggu)': opening_hours.get('Minggu', 'Tutup'), 'Link': restaurant['Link'],
                'Waktu Scraping': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }]

            timings['details'] = time.perf_counter() - start_details_time
            
            reviews = [] 
            start_reviews_time = time.perf_counter()
            try:
                print("   - Finding and clicking 'Cek ulasan'...")
                cek_ulasan_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//a[text()='Cek ulasan']"))
                )
                cek_ulasan_button.click()
                print("   - Navigated to reviews page.")
                
                time.sleep(3)

                click_count = 0
                while True: # Limit clicks to avoid infinite loops
                    try:
                        print(f"   - Attempting to click 'Muat lebih banyak' ({click_count+1}/2)...")
                        load_more_reviews_button = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.XPATH, '//button[.//span[text()="Muat lebih banyak"]]'))
                        )
                        driver.execute_script("arguments[0].click();", load_more_reviews_button)
                        click_count += 1
                        print("   - 'Muat lebih banyak' clicked.")
                        time.sleep(3) 
                    except (TimeoutException, ElementClickInterceptedException):
                        print("   - 'Muat lebih banyak' button not found or not clickable. Assuming all reviews are loaded.")
                        break 

                timings['reviews'] = time.perf_counter() - start_reviews_time
                
                last_scraped_timestamp_for_this_restaurant = datastore_manager.get_last_scraped_timestamp(restaurant_name)

                scraped_reviews_list, latest_review_date_this_scrape = self.scrape_reviews(driver, restaurant_name, last_scraped_timestamp_for_this_restaurant)
                
                reviews = scraped_reviews_list

                if reviews:
                    print(f"   - ✅ Successfully scraped {len(reviews)} NEW reviews for '{restaurant_name}'.")
                    if latest_review_date_this_scrape > last_scraped_timestamp_for_this_restaurant:
                        datastore_manager.update_last_scraped_timestamp(restaurant_name, latest_review_date_this_scrape)
                    else:
                        print(f"   - There are no newer reviews than {last_scraped_timestamp_for_this_restaurant} for ‘{restaurant_name}’. Timestamp not updated.")
                else:
                    print(f"   - ℹ️ No new reviews found for '{restaurant_name}'.")

            except (TimeoutException, NoSuchElementException):
                print(f"   - ⚠️ Could not find or click 'Cek ulasan' for {restaurant['Nama Restoran']}.")
            except Exception as e:
                print(f"   - ❌ An error occurred during review scraping: {e}")

            print(f"[{index}/{total}] ✅ THREAD FINISHED: Success scraping '{restaurant_name}'. Found {len(menu_items)} menu, {len(promo_items)} promo, {len(reviews)} ulasan.")
            return restaurant_details, menu_items, promo_items, reviews, timings
        
        except Exception as e:
            print(f"[{index}/{total}] ❌ THREAD ERROR for '{restaurant_name}': {e}")
            
            restaurant_details = [{
                'Nama Restoran': restaurant_name, 'Rating': 'N/A', 'Jarak': 'N/A', 'Tingkat Harga': 'N/A', 'Detail Harga': 'N/A',
                'Tipe Masakan': 'N/A', 'Alamat': 'N/A', 'Jam Buka (Senin)': 'Tutup', 'Jam Buka (Selasa)': 'Tutup',
                'Jam Buka (Rabu)': 'Tutup', 'Jam Buka (Kamis)': 'Tutup', 'Jam Buka (Jumat)': 'Tutup', 'Jam Buka (Sabtu)': 'Tutup',
                'Jam Buka (Minggu)': 'Tutup', 'Link': restaurant['Link'],
                'Waktu Scraping': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }]
            
            return restaurant_details, [], [], [], timings
        finally:
            if driver:
                driver.quit()
                print(f"   - WebDriver for {restaurant_name} closed.")