from typing import Any, Dict, List, Tuple, Generator
from urllib.parse import urljoin
import pandas as pd
import re
import time
import random
import concurrent.futures

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from scraper.selenium_handler import SeleniumHandler
from processor.email_extractor import EmailExtractor


class SeleniumScraper:
    def __init__(
        self,
        portal: str,
        config: Dict[str, Any],
        sector: str,
        max_pages: int,
        headless: bool = True,
    ):
        self.portal = portal
        self.sector = sector
        self.max_pages = max_pages
        self.headless = headless
        self.config = config
        self.email_extractor = EmailExtractor()
        # The main handler is now used only for logging and utility, not for a shared driver
        self.logger = SeleniumHandler().logger 

    def scrape(self) -> Dict[str, Any]:
        main_handler = SeleniumHandler()
        try:
            company_profiles, pages_scraped = self.extract_company_profiles(main_handler)
        finally:
            main_handler.close_driver()
        
        details, emails = self.extract_details_and_emails_parallel(company_profiles)

        return {
            "company_profiles": company_profiles,
            "details": details,
            "emails": emails,
            "pages_scraped": pages_scraped
        }

    def extract_company_profiles(self, handler: SeleniumHandler) -> Tuple[List[str], int]:
        # This method remains sequential for stable pagination
        links = set()
        base_url = self.config["base_url"]
        search_path_template = base_url + self.config["search_path_template"]
        driver = handler.setup_driver(headless=self.headless)
        current_page = 1
        while current_page <= self.max_pages:
            try:
                # wlw has a different URL structure for pagination.
                # Page 1 has no page param, subsequent pages are clicked.
                if self.portal == 'wlw' and current_page == 1:
                    page_url = search_path_template.format(sector=self.sector)
                elif self.portal == 'wlw' and current_page > 1:
                    # For subsequent pages, we rely on clicking the 'next' button,
                    # so we don't need to format the URL again. The driver is already on the right page.
                    pass 
                else:
                    page_url = search_path_template.format(sector=self.sector, page=current_page)

                self.logger.info(f"Scraping page {current_page} for portal {self.portal}")
                handler.random_delay()
                
                # Only call driver.get() if we have a new URL to navigate to.
                if not (self.portal == 'wlw' and current_page > 1):
                    driver.get(page_url)

                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, self.config["selectors"]["company_profiles"]))
                    )
                except TimeoutException:
                    self.logger.warning(f"Timeout waiting for company links on page {current_page}")
                    break
                
                soup = handler.parse_html(driver.page_source)
                company_elements = soup.select(self.config["selectors"]["company_profiles"])
                
                if not company_elements and current_page == 1:
                    self.logger.warning(f"No company profiles found on the first page for sector '{self.sector}'. Stopping.")
                    break

                print(f"Found {len(company_elements)} company profiles on page {current_page}")
                for element in company_elements:
                    profile_url = element.get("href")
                    if profile_url and not profile_url.startswith("http"):
                        profile_url = urljoin(base_url, profile_url)
                    if profile_url and profile_url not in links:
                        links.add(profile_url)
                
                # After scraping, try to move to the next page
                try:
                    next_page_selector = self.config["selectors"]["next_page"]
                    next_page_element = driver.find_element(By.CSS_SELECTOR, next_page_selector)
                    
                    # For wlw, we must click to paginate
                    if self.portal == 'wlw':
                        driver.execute_script("arguments[0].click();", next_page_element)
                    
                    current_page += 1

                except NoSuchElementException:
                    self.logger.warning(f"No next page button found on page {current_page}, stopping scraping.")
                    break

            except Exception as e:
                self.logger.error(f"Error scraping company profiles on page {current_page}: {e}")
                break
        return list(links), current_page -1

    def _process_single_profile(self, url_info: Tuple[int, str, int]) -> Tuple[Dict, Dict]:
        """Worker function for each thread. Scrapes a single company profile."""
        index, profile_url, total = url_info
        thread_handler = SeleniumHandler()
        profile_details = {'name': None, 'address': None, 'country': None, 'website': None, 'email_source': 'not_found'}
        website_emails = {'emails': []}
        
        try:
            driver = thread_handler.setup_driver(headless=self.headless)
            print(f"Visiting company profile [{index}/{total}]: {profile_url}")
            driver.get(profile_url)
            thread_handler.random_delay()

            # Handle cookie consent banner which may overlay other elements
            try:
                # This ID is common for Cookiebot "Accept All" buttons
                accept_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll"))
                )
                accept_button.click()
                self.logger.info(f"Accepted cookie banner on {profile_url}")
                time.sleep(2)  # Wait for banner to disappear
            except TimeoutException:
                self.logger.info(f"No cookie banner found on {profile_url}, proceeding.")

            # --- Extract details from profile page ---
            name_selector = self.config["selectors"]["company_name"]
            address_selector = self.config["selectors"]["company_address"]
            country_selector = self.config["selectors"]["country"]
            website_selector = self.config["selectors"]["website_links"]
            try:
                name_element = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, name_selector)))
                profile_details['name'] = name_element.text
            except TimeoutException: self.logger.warning(f"Company name not found on {profile_url}")
            try:
                address_element = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, address_selector)))
                profile_details['address'] = address_element.text
            except TimeoutException: self.logger.warning(f"Address not found on {profile_url}")
            try:
                country_element = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, country_selector)))
                profile_details['country'] = country_element.text
            except TimeoutException: self.logger.warning(f"Country not found on {profile_url}")
            
            # --- Get website and extract emails ---
            website_url = None
            try:
                if self.portal == "wlw":
                    # For 'wlw', we need to click a button to reveal the website link (<a> tag).
                    # The initial element is a <button>, which then becomes an <a> tag.
                    # We derive the button selector from the configured link selector.
                    link_selector = self.config["selectors"]["website_links"]
                    button_selector = link_selector.replace('a.', 'button.', 1)

                    # 1. Find and click the button.
                    website_button = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, button_selector))
                    )
                    website_button.click()
                    
                    # 2. Now wait for the <a> tag to be present and get the href.
                    website_element = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, link_selector))
                    )
                else:
                    website_element = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, website_selector)))
                
                website_url = website_element.get_attribute('href')
                profile_details['website'] = website_url
            except TimeoutException: self.logger.warning(f"Website link not found on {profile_url}")

            if website_url:
                try:
                    driver.get(website_url)
                    # Wait for the page to be fully loaded, especially for JS-heavy sites
                    WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                    thread_handler.random_delay()
                    page_source = driver.page_source
                    found_emails = self.email_extractor.extract_and_filter_emails(page_source, 'html', website_url)
                    if found_emails:
                        profile_details['email_source'] = 'main_page'

                    if not found_emails:
                        contact_keywords = ['contact', 'kontakt', 'iletişim', 'contacto', 'contatto']
                        contact_page_url = None
                        for keyword in contact_keywords:
                            try:
                                contact_link = driver.find_element(By.XPATH, f"//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{keyword}')]")
                                contact_page_url = contact_link.get_attribute('href')
                                if contact_page_url:
                                    if not contact_page_url.startswith('http'):
                                        contact_page_url = urljoin(website_url, contact_page_url)
                                    break
                            except NoSuchElementException: continue
                        if contact_page_url:
                            driver.get(contact_page_url)
                            # Wait for the contact page to be fully loaded
                            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                            thread_handler.random_delay()
                            page_source = driver.page_source
                            found_emails = self.email_extractor.extract_and_filter_emails(page_source, 'html', contact_page_url)
                            if found_emails:
                                profile_details['email_source'] = 'contact_page'

                    website_emails['emails'] = found_emails
                except WebDriverException as e:
                    if "net::ERR_NAME_NOT_RESOLVED" in e.msg:
                        self.logger.warning(f"Could not resolve domain name for {website_url}. The website may be down or incorrect. Skipping.")
                    else:
                        # Log other, unexpected WebDriver errors more verbosely but without crashing
                        self.logger.error(f"A WebDriver error occurred for {website_url}: {e.msg}")
                except Exception as e: self.logger.error(f"An unexpected error occurred while scraping emails from {website_url}: {e}")

        except Exception as e:
            self.logger.error(f"Error processing profile {profile_url}: {e}")
        finally:
            thread_handler.close_driver()
            
        return profile_details, website_emails

    def extract_details_and_emails_parallel(self, company_profiles: List[str]) -> Tuple[List[Dict[str, str]], List[Dict[str, List[str]]]]:
        details = []
        emails = []
        total_profiles = len(company_profiles)
        # Create a list of tuples with (index, url, total) for the worker
        urls_with_info = [(i + 1, url, total_profiles) for i, url in enumerate(company_profiles)]

        # Using max_workers=7 as a default, can be made configurable
        with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
            # map preserves the order of the input list
            results = list(executor.map(self._process_single_profile, urls_with_info))

        for res_details, res_emails in results:
            details.append(res_details)
            emails.append(res_emails)
            
        return details, emails
        