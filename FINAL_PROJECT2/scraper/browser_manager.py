import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

class WebDriverManager:
    """
    WebDriverManager is a helper class to configure and initialize Selenium WebDriver
    with predefined options for automated web interactions.

    This class provides an easy-to-use interface for setting up Chrome WebDriver
    with optional headless mode and custom browser settings like geolocation permissions
    and logging suppression.

    Methods:
        setup_driver(headless: bool = True) -> webdriver.Chrome | None:
            Initializes and returns a configured Chrome WebDriver instance.
            If initialization fails, returns None.
    """


    def __init__(self) -> None:
        """
        Initializes an instance of WebDriverManager.

        Since no configuration is needed upon instantiation, this constructor is currently empty.
        """
        pass
    

    def setup_driver(self, headless=True) -> webdriver.Chrome | None:
        """
        Initializes the Chrome WebDriver with specific options such as headless mode,
        geolocation permissions, and logging suppression.

        Parameters:
            headless (bool, optional): Whether to run the browser in headless mode.
                                       Defaults to True.

        Returns:
            webdriver.Chrome: A configured Chrome WebDriver instance ready for use.
            None: If the WebDriver initialization fails due to an exception.

        Raises:
            Exception: Captures any exceptions during WebDriver setup and prints an error message.
        """
        try:
            print(f"🚀 Initializing WebDriver (Headless: {headless})...")
            options = webdriver.ChromeOptions()
            options.add_experimental_option("prefs", {
                "profile.default_content_setting_values.geolocation": 1  # 1: Allow, 2: Block
            })

            options.add_argument('--log-level=3') # Only fatal errors
            options.add_experimental_option('excludeSwitches', ['enable-logging'])
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