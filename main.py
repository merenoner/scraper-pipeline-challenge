import os
# Suppress TensorFlow logs
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

import json
import argparse
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from scraper.selenium_scraper import SeleniumScraper
from scraper.requests_scraper import RequestsScraper
from processor.data_processor import DataProcessor

def load_config(portal: str) -> Dict[str, Any]:
    """
    Loads the configuration for the given portal.
    """
    config_path = Path(__file__).parent / "config" / "configuration.json"
    try:
        with open(config_path, "r") as f:
            return json.load(f)["portals"][portal]
    except Exception as e:
        raise Exception(f"Error loading configuration for {portal}: {e}")

def run_pipeline(portal: str, sector: str, max_pages: int, headless: bool = True):
    """
    Args:
        portal (str): Name of the portal to scrape
        sector (str): Name of the sector to scrape
        max_pages (int): Max pages to scrape
        headless (bool, optional): Whether to run the browser in headless mode. Defaults to True.
    """
    config = load_config(portal)
    
    # --- Scraper Factory ---
    scraper_params = {
        "portal": portal,
        "config": config,
        "sector": sector,
        "max_pages": max_pages,
        "headless": headless,
    }

    if config['engine'] == 'selenium':
        scraper = SeleniumScraper(**scraper_params)
    elif config['engine'] == 'requests':
        scraper = RequestsScraper(**scraper_params)
    else:
        raise ValueError(f"Unsupported engine: {config['engine']}")
    # --------------------

    try:
        scraped_data = scraper.scrape()
        processor = DataProcessor()

        # Create data directory if it doesn't exist
        output_dir = Path("data")
        output_dir.mkdir(exist_ok=True)

        # 1. Save company profile links
        links_filename = output_dir / f"links_{sector}.csv"
        profile_links = [{"profile_url": url} for url in scraped_data["company_profiles"]]
        processor.save_to_csv(profile_links, str(links_filename))
        print(f"Successfully saved {len(profile_links)} profile links to {links_filename}")

        # Combine all scraped data into a structured list of records
        records = []
        for i in range(len(scraped_data["details"])):
            details = scraped_data["details"][i]
            emails = scraped_data["emails"][i].get("emails", [])
            profile_url = scraped_data["company_profiles"][i]
            
            records.append({
                "Name": details.get("name"),
                "Country": details.get("country"),
                "Address": details.get("address"),
                "Website": details.get("website"),
                "Email": ", ".join(emails),
                "Profile_URL": profile_url
            })

        # 2. Save emails with company name and country
        emails_filename = output_dir / f"emails_{sector}.csv"
        email_records = []
        for record in records:
            if record.get("Email"):
                email_records.append({
                    "Name": record.get("Name"),
                    "Country": record.get("Country"),
                    "Email": record.get("Email")
                })
        processor.save_to_csv(email_records, str(emails_filename))
        print(f"Successfully saved {len(email_records)} records with emails to {emails_filename}")

        # 3. Process and save detailed data
        detailed_filename = output_dir / f"detailed_{sector}.csv"
        processed_data = processor.process_scraped_data(records)
        processor.save_to_csv(processed_data, str(detailed_filename))
        print(f"Successfully saved {len(processed_data)} processed records to {detailed_filename}")

        # Get and print statistics for the main detailed file
        stats = processor.get_data_statistics(processed_data)
        print(f"\n--- Data Statistics for {detailed_filename} ---")
        for key, value in stats.items():
            print(f"{key}: {value}")
        print("--------------------------------------------------\n")

    finally:
        # Close the driver only if it's a selenium scraper and the handler exists
        if config['engine'] == 'selenium' and hasattr(scraper, 'handler'):
            scraper.handler.close_driver()
        # No need for an else, as RequestsScraper doesn't need cleanup

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper Pipeline")
    parser.add_argument("--portal", type=str, required=True, help="Name of the portal to scrape (e.g., europages)")
    parser.add_argument("--sector", type=str, required=True, help="Name of the sector to scrape")
    parser.add_argument("--max_pages", type=int, required=True, help="Max number of pages to scrape")
    parser.add_argument("--no-headless", action="store_false", dest="headless", help="Disable headless mode for the browser")
    
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    try:
        run_pipeline(
            portal=args.portal,
            sector=args.sector,
            max_pages=args.max_pages,
            headless=args.headless
        )
    except Exception as e:
        logging.error(f"An error occurred during the pipeline execution: {e}")
        sys.exit(1)

    