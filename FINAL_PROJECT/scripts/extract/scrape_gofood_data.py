import os
import time
import pandas as pd
import concurrent.futures
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException, NoSuchElementException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

workers = os.cpu_count()
workers

def setup_driver(headless=True):
    """
    Initializes and configures the Selenium Chrome WebDriver.
    
    This function sets up Chrome options to automatically allow location access
    and uses webdriver-manager to handle the driver installation.
    
    Returns:
        webdriver.Chrome: The configured WebDriver instance, or None if initialization fails.
    """
    try:
        print(f"🚀 Initializing WebDriver (Headless: {headless})...")
        options = webdriver.ChromeOptions()
        options.add_experimental_option("prefs", {
            "profile.default_content_setting_values.geolocation": 1  # 1: Allow, 2: Block
        })

        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")

        if headless:
            options.add_argument("--headless")
            options.add_argument("--window-size=1920,1080")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        print("👍 WebDriver successfully initialized.")
        return driver
    except Exception as e:
        print(f"❌ Failed to initialize WebDriver: {e}")
        return None
    
def initial_navigation(driver, url):
    """
    Navigates to the GoFood website and reaches the 'Terdekat' (Nearest) category page.
    
    Args:
        driver (webdriver.Chrome): The active WebDriver instance.
        url (str): The GoFood URL to open.
    """
    print(f"\n🌍 Opening URL: {url}")
    driver.get(url)

    print("🔎 Finding and clicking the location input field...")
    location_input = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((By.ID, "location-picker"))
    )
    location_input.click()
    print("👍 Location input field clicked.")

    print("📍 Clicking 'Use your current location' button...")
    use_current_location_button = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((By.XPATH, '//*[contains(text(), "Pakai lokasimu saat ini")]'))
    )
    use_current_location_button.click()
    print("👍 'Use your current location' button clicked.")

    print("🧭 Clicking the 'Explore' button...")
    explore_button = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((By.XPATH, '//button[contains(., "Eksplor")]'))
    )
    explore_button.click()
    print("👍 'Explore' button clicked.")
    
    time.sleep(5) 

    print("🏠 Finding and clicking the 'Terdekat' category...")
    terdekat_category_button = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((By.XPATH, "//h3[@title='Terdekat']"))
    )
    terdekat_category_button.click()
    print("👍 'Terdekat' category successfully clicked.")

def get_all_distances_from_html(driver):
    soup = BeautifulSoup(driver.page_source, 'html.parser')
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

def scroll_and_load_all_data(driver, target_distance_km=10.0):
    last_known_distance = 0.0

    while True:
        try:
            all_distances = get_all_distances_from_html(driver)

            if all_distances:
                current_max_distance = max(all_distances)
                last_known_distance = current_max_distance
                print(f"  📍 Current longest distance: {current_max_distance:.2f} km")

                if current_max_distance >= target_distance_km:
                    print(f"\n✅ Target reached! Found a restaurant ≥ {target_distance_km} km. Process stopped.")
                    return
            else:
                print("  ⚠️ No distance (km) elements were found on the current page.")

        except Exception as e:
            print(f"  ❌ An error occurred while parsing HTML: {type(e).__name__} - {e}")
            time.sleep(1)
            continue

        print(f"  ➡️ Still below target ({last_known_distance:.2f} km < {target_distance_km} km), scroll again...")

        button_clicked = False
        max_scroll_attempts = 1

        for attempt in range(max_scroll_attempts):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.5)

            try:
                load_more_button = WebDriverWait(driver, 2).until(
                    EC.element_to_be_clickable((By.XPATH, '//button[.//span[text()="Muat lebih banyak"]]'))
                )
                load_more_button.click()
                print("  👍 The ‘Load more’ button was found and clicked.")
                button_clicked = True
                time.sleep(3)
                break
            except TimeoutException:
                print(f"    🔍 The button has not appeared yet (attempt {attempt + 1}/{max_scroll_attempts})... scroll again.")
                continue

        if not button_clicked:
            print("\n⚠️ End of list. The ‘Load more’ button was not found after scrolling several times.")
            break

    print("\n⏹️ Done: No restaurants found within the target distance.")

def scrape_restaurant_list(driver):
    """
    Parses the page source and extracts the initial list of restaurants.
    
    Args:
        driver (webdriver.Chrome): The active WebDriver instance.
        
    Returns:
        list: A list of dictionaries, where each dictionary contains the initial details of one restaurant.
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

def scrape_reviews(driver, restaurant_name):
    """
    Scrapes all loaded reviews from the review page.
    """
    print("   - Scraping reviews...")
    all_reviews_data = []
    
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'flex flex-col space-y-10')]"))
        )
    except TimeoutException:
        print("   - ⚠️ No review container found on the page.")
        return []

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    review_container = soup.find('div', class_=lambda c: c and 'flex' in c and 'flex-col' in c and 'space-y-10' in c)
    
    if not review_container:
        print("   - ⚠️ Could not find review container.")
        return []

    review_cards = review_container.find_all('div', recursive=False)
    print(f"   - Found {len(review_cards)} reviews to parse.")

    for card in review_cards:
        try:
            name_tag = card.find('h3', class_='text-gf-content-secondary gf-label-m')
            name = name_tag.text.strip() if name_tag else 'N/A'

            since_tag = card.find('span', class_='mt-1 text-gf-content-muted gf-body-xs md:gf-body-s')
            since = since_tag.text.replace('Pengguna Gojek sejak', '').strip() if since_tag else 'N/A'

            rating_tag = card.find('span', class_='ml-1 inline-block')
            rating = rating_tag.text.strip() if rating_tag else 'N/A'
            
            review_text_tag = card.find('p', class_='break-words gf-body-m')
            review_text = review_text_tag.text.strip() if review_text_tag else ''

            product_tag = card.find('span', class_='ml-2 break-words md:mt-1')
            products = product_tag.text.strip() if product_tag else 'N/A'
            
            bought_date_tag = card.find('div', class_='mt-4 text-gf-content-muted gf-body-s')
            bought_date = bought_date_tag.text.replace('Dibeli tanggal', '').strip() if bought_date_tag else 'N/A'
            
            all_reviews_data.append({
                'Nama Restoran': restaurant_name,
                'Nama': name,
                'Pengguna Gojek Sejak': since,
                'Rating': rating,
                'Ulasan': review_text,
                'Produk yang Dibeli': products,
                'Tanggal Beli': bought_date,
                'Waktu Scraping': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        except Exception as e:
            print(f"   - ❗️ Error parsing a review card: {e}")
            continue

    return all_reviews_data

def scrape_menu(driver, restaurant_name):
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

def scrape_promos(driver, restaurant_name):
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

def get_single_restaurant_details(restaurant, index, total):
    driver = None
    restaurant_name = restaurant['Nama Restoran']
    print(f"[{index}/{total}] THREAD START: Scraping '{restaurant_name}'...")
        
    try:
        driver = setup_driver()
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
        address, opening_hours = "Alamat tidak ditemukan", {}
        
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

        menu_items = scrape_menu(driver, restaurant['Nama Restoran'])
        if menu_items:
            print(f"   - ✅ Successfully scraped {len(menu_items)} menu items.")

        promo_items = scrape_promos(driver, restaurant['Nama Restoran'])
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

        try:
            print("   - Finding and clicking 'Cek ulasan'...")
            cek_ulasan_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//a[text()='Cek ulasan']"))
            )
            cek_ulasan_button.click()
            print("   - Navigated to reviews page.")
            
            time.sleep(3)

            click_count = 0
            while True:
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
            
            reviews = scrape_reviews(driver, restaurant['Nama Restoran'])
            if reviews:
                print(f"   - ✅ Successfully scraped {len(reviews)} reviews.")

        except (TimeoutException, NoSuchElementException):
            print(f"   - ⚠️ Could not find or click 'Cek ulasan' for {restaurant['Nama Restoran']}.")
        except Exception as e:
            print(f"   - ❌ An error occurred during review scraping: {e}")

        print(f"[{index}/{total}] ✅ THREAD FINISHED: Success scraping '{restaurant_name}'. Found {len(menu_items)} menu, {len(promo_items)} promo, {len(reviews)} ulasan.")
        return restaurant_details, menu_items, promo_items, reviews
    
    except Exception as e:
        print(f"[{index}/{total}] ❌ THREAD ERROR for '{restaurant_name}': {e}")
        print(f"   ❌ An unexpected error occurred for {restaurant['Nama Restoran']}: {e}")

        restaurant_details = [{
            'Nama Restoran': restaurant['Nama Restoran'], 'Rating': rating, 'Jarak': distance,
            'Tingkat Harga': price_str, 'Detail Harga': price_detail, 'Alamat': address,
            'Jam Buka (Senin)': opening_hours.get('Senin', 'Tutup'), 'Jam Buka (Selasa)': opening_hours.get('Selasa', 'Tutup'),
            'Jam Buka (Rabu)': opening_hours.get('Rabu', 'Tutup'), 'Jam Buka (Kamis)': opening_hours.get('Kamis', 'Tutup'),
            'Jam Buka (Jumat)': opening_hours.get('Jumat', 'Tutup'), 'Jam Buka (Sabtu)': opening_hours.get('Sabtu', 'Tutup'),
            'Jam Buka (Minggu)': opening_hours.get('Minggu', 'Tutup'), 'Link': restaurant['Link'],
            'Waktu Scraping': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }]
        
        time.sleep(1)

        return restaurant_details, [], [], []
    finally:
        if driver:
            driver.quit()
            print(f"   - WebDriver for {restaurant_name} closed.")
        else:
            print(f"   - No WebDriver to close for {restaurant_name}.")

def get_details_multithreading(initial_data_list, max_workers=5):
    """
    Scrapes restaurant details, reviews, menu items, and promotions using multithreading.
    
    Args:
        initial_data_list (list): List of dictionaries containing initial restaurant data.
        max_workers (int): Maximum number of threads to use for scraping.
        
    Returns:
        tuple: Four DataFrames containing restaurant details, reviews, menu items, and promotions.
    """
    detailed_results = []
    all_reviews_results = []
    all_menu_results = []
    all_promos_results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_single_restaurant_details, restaurant, index + 1, len(initial_data_list)): index for index, restaurant in enumerate(initial_data_list)}
        
        for future in concurrent.futures.as_completed(futures):
            index = futures[future]
            try:
                restaurant_details, menu_items, promo_items, reviews = future.result()

                if restaurant_details:
                    detailed_results.extend(restaurant_details)
                
                if reviews:
                    all_reviews_results.extend(reviews)
                
                if menu_items:
                    all_menu_results.extend(menu_items)

                if promo_items:
                    all_promos_results.extend(promo_items)
            except Exception as e:
                print(f"[{index + 1}/{len(initial_data_list)}] ❌ Error processing restaurant {index + 1}: {e}")    
                restaurant_name = futures[future]['Nama Restoran']
                print(f"❌ An exception was raised by the thread for '{restaurant_name}': {e}")

    df_restaurants = pd.DataFrame(detailed_results)
    df_reviews = pd.DataFrame(all_reviews_results)     
    df_menu = pd.DataFrame(all_menu_results)
    df_promos = pd.DataFrame(all_promos_results)

    return df_restaurants, df_reviews, df_menu, df_promos

driver = None
try:
    driver = setup_driver()
    if driver:
        gofood_url = "https://gofood.co.id/id"
        initial_navigation(driver, gofood_url)
        scroll_and_load_all_data(driver)
        
        initial_restaurant_list = scrape_restaurant_list(driver)
        
        if initial_restaurant_list:
            # detailed_data, reviews_data, menu_data, promo_data = get_full_restaurant_details_and_reviews(driver, initial_restaurant_list)

            detailed_data, reviews_data, menu_data, promo_data = get_details_multithreading(initial_restaurant_list, max_workers=5)
            
            output_dir = "/app/data" 
            os.makedirs(output_dir, exist_ok=True)

            detailed_data.to_csv(os.path.join(output_dir, 'hasil_detail_restoran.csv'), index=False)
            reviews_data.to_csv(os.path.join(output_dir, 'hasil_ulasan_pelanggan.csv'), index=False)
            menu_data.to_csv(os.path.join(output_dir, 'hasil_menu_restoran.csv'), index=False)
            promo_data.to_csv(os.path.join(output_dir, 'hasil_promo_restoran.csv'), index=False)
            # detailed_data.to_csv('hasil_detail_restoran.csv', index=False)
            
            # reviews_data.to_csv('hasil_ulasan_pelanggan.csv', index=False)
            
            # menu_data.to_csv('hasil_menu_restoran.csv', index=False)
            
            # promo_data.to_csv('hasil_promo_restoran.csv', index=False)
        else:
            print("No restaurants were found to process further.")

except Exception as e:
    print(f"\n❌ An unexpected error occurred during the main process: {e}")
    
finally:
    if driver:
        print("\n🚪 Process finished, closing the browser.")
        driver.quit()