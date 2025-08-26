import logging
import time
import random
import os
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

class SeleniumHandler:
    def __init__(self):
        self.driver = None
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)
        return logger

    def random_delay(self, min_seconds=1, max_seconds=3):
        time.sleep(random.uniform(min_seconds, max_seconds))

    def parse_html(self, html_content: str) -> BeautifulSoup:
        """Parses HTML content using BeautifulSoup."""
        return BeautifulSoup(html_content, 'lxml')

    def setup_driver(self, headless: bool = True) -> webdriver.Chrome:
        """
        Sets up the Chrome WebDriver using Selenium's built-in driver manager.
        """
        options = Options()
        if headless:
            options.add_argument('--headless')

        # Disable images to speed up page loading
        prefs = {"profile.managed_default_content_settings.images": 2}
        options.add_experimental_option("prefs", prefs)

        # Suppress browser and driver logs
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_argument('--log-level=3')

        options.add_argument("--no-sandbox")
        options.add_argument("user-agent=Mozilla.5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

        # Redirect chromedriver service logs to null device
        service = Service(log_path=os.devnull)

        self.driver = webdriver.Chrome(service=service, options=options)
        return self.driver

    def close_driver(self):
        """
        Closes the WebDriver.
        """
        if self.driver:
            self.driver.quit()
            self.driver = None
