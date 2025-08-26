import requests
from bs4 import BeautifulSoup
from typing import Any, Dict, List, Tuple
from urllib.parse import urljoin
import logging
import time
import random
import re

from processor.email_extractor import EmailExtractor

class RequestsScraper:
    def __init__(
        self,
        portal: str,
        config: Dict[str, Any],
        sector: str,
        max_pages: int,
        **kwargs  # Accept headless and other params but ignore them
    ):
        self.portal = portal
        self.config = config
        self.sector = sector
        self.max_pages = max_pages
        self.email_extractor = EmailExtractor()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        })
        self.logger = logging.getLogger(self.__class__.__name__)

    def _random_delay(self):
        time.sleep(random.uniform(0.5, 1.5))

    def scrape(self) -> Dict[str, Any]:
        company_profiles = self.extract_company_profiles()
        details, emails = self.extract_details_and_emails(company_profiles)
        return {
            "company_profiles": company_profiles,
            "details": details,
            "emails": emails,
        }

    def extract_company_profiles(self) -> List[str]:
        links = set()
        base_url = self.config["base_url"]
        search_path_template = base_url + self.config["search_path_template"]
        current_page = 1
        
        while current_page <= self.max_pages:
            page_url = search_path_template.format(sector=self.sector, page=current_page)
            self.logger.info(f"Scraping page {current_page}: {page_url}")
            try:
                response = self.session.get(page_url, timeout=10)
                response.raise_for_status()
                self._random_delay()
                
                soup = BeautifulSoup(response.text, 'lxml')
                company_elements = soup.select(self.config["selectors"]["company_profiles"])
                print(f"Found {len(company_elements)} company profiles on page {current_page}")

                for element in company_elements:
                    profile_url = element.get("href")
                    if profile_url and not profile_url.startswith("http"):
                        profile_url = urljoin(base_url, profile_url)
                    if profile_url and profile_url not in links:
                        links.add(profile_url)

                next_page_element = soup.select_one(self.config["selectors"]["next_page"])
                if not next_page_element:
                    self.logger.warning(f"No next page found on page {current_page}, stopping scraping")
                    break
                current_page += 1
            except requests.RequestException as e:
                self.logger.error(f"Failed to fetch {page_url}: {e}")
                break
        return list(links)

    def extract_details_and_emails(self, company_profiles: List[str]) -> Tuple[List[Dict[str, str]], List[Dict[str, List[str]]]]:
        details = []
        emails = []

        for profile_url in company_profiles:
            print(f"Visiting company profile: {profile_url}")
            profile_details = {'name': None, 'address': None, 'country': None, 'website': None}
            website_emails = {'emails': []}
            
            try:
                response = self.session.get(profile_url, timeout=10)
                response.raise_for_status()
                self._random_delay()

                soup = BeautifulSoup(response.text, 'lxml')
                
                name_selector = self.config["selectors"]["company_name"]
                address_selector = self.config["selectors"]["company_address"]
                country_selector = self.config["selectors"]["country"]
                website_selector = self.config["selectors"]["website_links"]

                if name_el := soup.select_one(name_selector): profile_details['name'] = name_el.text.strip()
                if address_el := soup.select_one(address_selector): profile_details['address'] = address_el.text.strip()
                if country_el := soup.select_one(country_selector): profile_details['country'] = country_el.text.strip()

                website_url = None
                if website_el := soup.select_one(website_selector):
                    website_url = website_el.get('href')
                    profile_details['website'] = website_url

                if website_url:
                    try:
                        web_response = self.session.get(website_url, timeout=15)
                        web_response.raise_for_status()
                        self._random_delay()
                        
                        found_emails = self.email_extractor.extract_and_filter_emails(web_response.text, 'html', website_url)
                        
                        if not found_emails:
                            # Search for contact page links on the website's soup
                            contact_keywords = ['contact', 'kontakt', 'iletişim', 'contacto', 'contatto']
                            contact_page_url = None
                            for keyword in contact_keywords:
                                contact_link = soup.find('a', text=re.compile(keyword, re.I))
                                if contact_link and contact_link.get('href'):
                                    contact_page_url = urljoin(website_url, contact_link['href'])
                                    self.logger.info(f"Found contact page: {contact_page_url}")
                                    break
                            
                            if contact_page_url:
                                contact_response = self.session.get(contact_page_url, timeout=15)
                                found_emails = self.email_extractor.extract_and_filter_emails(contact_response.text, 'html', contact_page_url)

                        website_emails['emails'] = found_emails
                    except requests.RequestException as e:
                        self.logger.error(f"Could not fetch company website {website_url}: {e}")

            except requests.RequestException as e:
                self.logger.error(f"Could not fetch profile {profile_url}: {e}")

            details.append(profile_details)
            emails.append(website_emails)
        
        return details, emails
