import pandas as pd
import logging
from typing import List, Dict, Any, Set
import re
from urllib.parse import urlparse
import validators


class DataProcessor:
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def clean_company_name(self, name: str) -> str:
        """
        Clean and normalize company name.
        
        Args:
            name: Raw company name
            
        Returns:
            Cleaned company name
        """
        if not name or name.lower() in ['unknown', 'n/a', '']:
            return 'Unknown'
            
        # Remove extra whitespace
        name = ' '.join(name.split())
        
        # Remove common suffixes and prefixes that might be noise
        noise_patterns = [
            r'^(Company:|Business:|Enterprise:)\s*',
            r'\s*-\s*(Company|Business|Enterprise)$',
            r'\s*\|\s*.*$',  # Remove everything after |
        ]
        
        for pattern in noise_patterns:
            name = re.sub(pattern, '', name, flags=re.IGNORECASE)
            
        return name.strip()
        
    def clean_country(self, country: str) -> str:
        """
        Clean and normalize country name.
        
        Args:
            country: Raw country string
            
        Returns:
            Cleaned country name
        """
        if not country or country.lower() in ['unknown', 'n/a', '']:
            return 'Unknown'
            
        # Country mapping for common variations
        country_mapping = {
            'DE': 'Germany',
            'FR': 'France', 
            'IT': 'Italy',
            'ES': 'Spain',
            'GB': 'United Kingdom',
            'UK': 'United Kingdom',
            'NL': 'Netherlands',
            'BE': 'Belgium',
            'AT': 'Austria',
            'CH': 'Switzerland',
            'PL': 'Poland',
            'CZ': 'Czech Republic',
            'HU': 'Hungary',
            'RO': 'Romania',
            'BG': 'Bulgaria',
            'HR': 'Croatia',
            'SI': 'Slovenia',
            'SK': 'Slovakia',
            'PT': 'Portugal',
            'GR': 'Greece',
            'DK': 'Denmark',
            'SE': 'Sweden',
            'NO': 'Norway',
            'FI': 'Finland',
            'IE': 'Ireland',
            'LU': 'Luxembourg',
            'EE': 'Estonia',
            'LV': 'Latvia',
            'LT': 'Lithuania'
        }
        
        country = country.strip()
        
        # Check if it's a country code
        if country.upper() in country_mapping:
            return country_mapping[country.upper()]
            
        # Capitalize properly
        return country.title()
        
    def validate_email(self, email: str) -> bool:
        """
        Validate email address.
        
        Args:
            email: Email to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not email:
            return False
            
        # Use validators library
        return validators.email(email)
        
    def validate_url(self, url: str) -> bool:
        """
        Validate URL.
        
        Args:
            url: URL to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not url:
            return False
            
        return validators.url(url)
        
    def deduplicate_records(self, records: List[Dict[str, Any]], 
                          key_fields: List[str] = None) -> List[Dict[str, Any]]:
        """
        Remove duplicate records based on key fields.
        
        Args:
            records: List of record dictionaries
            key_fields: Fields to use for deduplication (default: ['Name', 'Email'])
            
        Returns:
            List of deduplicated records
        """
        if not records:
            return []
            
        if key_fields is None:
            key_fields = ['Name', 'Email']
            
        seen = set()
        deduplicated = []
        
        for record in records:
            # Create a key from the specified fields
            key_values = []
            for field in key_fields:
                value = record.get(field, '').strip().lower()
                key_values.append(value)
            key = tuple(key_values)
            
            if key not in seen:
                seen.add(key)
                deduplicated.append(record)
            else:
                self.logger.debug(f"Duplicate record found: {key}")
                
        self.logger.info(f"Removed {len(records) - len(deduplicated)} duplicate records")
        return deduplicated
        
    def filter_invalid_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter out records with invalid data.
        
        Args:
            records: List of record dictionaries
            
        Returns:
            List of valid records
        """
        valid_records = []
        
        for record in records:
            is_valid = True
            
                
            # Check if company name is meaningful
            name = record.get('Name', '').strip()
            if not name or name.lower() in ['unknown', 'n/a', '', 'null']:
                self.logger.debug(f"Invalid company name: {name}")
                is_valid = False
                
            # Check if source URL is valid (if present)
            if 'Source_URL' in record and not self.validate_url(record['Source_URL']):
                self.logger.debug(f"Invalid source URL: {record.get('Source_URL', '')}")
                # Don't invalidate the record for this, just log it
                
            if is_valid:
                valid_records.append(record)
                
        self.logger.info(f"Filtered out {len(records) - len(valid_records)} invalid records")
        return valid_records
        
    def clean_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Clean all records in the dataset.
        
        Args:
            records: List of record dictionaries
            
        Returns:
            List of cleaned records
        """
        cleaned_records = []
        
        for record in records:
            cleaned_record = record.copy()
            
            # Clean company name
            if 'Name' in cleaned_record:
                cleaned_record['Name'] = self.clean_company_name(cleaned_record['Name'])
                
            # Clean country
            if 'Country' in cleaned_record:
                cleaned_record['Country'] = self.clean_country(cleaned_record['Country'])
                
            # Clean email (basic cleanup)
            if 'Email' in cleaned_record:
                email = cleaned_record['Email'].strip().lower()
                cleaned_record['Email'] = email
                
            cleaned_records.append(cleaned_record)
            
        return cleaned_records
        
    def process_scraped_data(self, records: List[Dict[str, Any]], 
                           remove_duplicates: bool = True,
                           filter_invalid: bool = True) -> List[Dict[str, Any]]:
        """
        Main method to process scraped data.
        
        Args:
            records: List of scraped record dictionaries
            remove_duplicates: Whether to remove duplicate records
            filter_invalid: Whether to filter out invalid records
            
        Returns:
            List of processed records
        """
        self.logger.info(f"Processing {len(records)} scraped records")
        
        # Step 1: Clean records
        cleaned_records = self.clean_records(records)
        self.logger.info(f"Cleaned {len(cleaned_records)} records")
        
        # Step 2: Filter invalid records
        if filter_invalid:
            cleaned_records = self.filter_invalid_records(cleaned_records)
            
        # Step 3: Remove duplicates
        if remove_duplicates:
            cleaned_records = self.deduplicate_records(cleaned_records)
            
        self.logger.info(f"Final processed dataset: {len(cleaned_records)} records")
        return cleaned_records
        
    def save_to_csv(self, records: List[Dict[str, Any]], filename: str, 
                   columns: List[str] = None) -> bool:
        """
        Save records to CSV file.
        
        Args:
            records: List of record dictionaries
            filename: Output filename
            columns: Specific columns to include (optional)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not records:
                self.logger.warning(f"No records to save to {filename}")
                return False
                
            df = pd.DataFrame(records)
            
            if columns:
                # Ensure columns exist
                existing_columns = [col for col in columns if col in df.columns]
                df = df[existing_columns]
                
            df.to_csv(filename, index=False)
            self.logger.info(f"Saved {len(df)} records to {filename}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving to {filename}: {str(e)}")
            return False
            
    def get_data_statistics(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Get statistics about the processed data.
        
        Args:
            records: List of processed records
            
        Returns:
            Dictionary with statistics
        """
        if not records:
            return {'total_records': 0}
            
        df = pd.DataFrame(records)
        
        stats = {
            'total_records': len(df),
            'unique_companies': df['Name'].nunique() if 'Name' in df.columns else 0,
            'unique_emails': df['Email'].nunique() if 'Email' in df.columns else 0,
            'countries': df['Country'].value_counts().to_dict() if 'Country' in df.columns else {},
            'has_duplicates': len(df) != len(df.drop_duplicates()),
        }
            
        return stats
