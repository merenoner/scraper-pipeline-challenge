"""
Email extraction utilities for finding and validating email addresses.
"""

import re
import logging
from typing import List, Set, Dict, Optional
from urllib.parse import urljoin, urlparse
import validators

from bs4 import BeautifulSoup


class EmailExtractor:
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Common email regex patterns
        self.email_patterns = [
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',  # Standard email
            r'\b[A-Za-z0-9._%+-]+\s*@\s*[A-Za-z0-9.-]+\s*\.\s*[A-Z|a-z]{2,}\b',  # With spaces
            r'\b[A-Za-z0-9._%+-]+\[at\][A-Za-z0-9.-]+\[dot\][A-Z|a-z]{2,}\b',  # Obfuscated
            r'\b[A-Za-z0-9._%+-]+\s*\(at\)\s*[A-Za-z0-9.-]+\s*\(dot\)\s*[A-Z|a-z]{2,}\b',  # Obfuscated with parentheses
        ]
        
        # Compile patterns for efficiency
        self.compiled_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in self.email_patterns]
        
        # Common business email domains (prioritize these)
        self.business_domains = {
            'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
            'icloud.com', 'me.com', 'live.com', 'msn.com', 'ymail.com'
        }
        
        # Domains to exclude (usually not business emails)
        self.excluded_domains = {
            # Common test/example domains
            'example.com', 'test.com', 'localhost', 'domain.com', 'mydomain.com',
            
            # Social media and tech giants
            'google.com', 'facebook.com', 'twitter.com', 'linkedin.com',
            'instagram.com', 'youtube.com', 'microsoft.com', 'apple.com',
            
            # Analytics and tracking services
            'criteo.com', 'criteo.com.we', 'criteo.net',
            'reppublika.com', 'reppublika.com.reppublika',
            'visable.com', 'visable.comvisable',
            'googletagmanager.com', 'google-analytics.com',
            'hotjar.com', 'mixpanel.com', 'segment.com',
            'intercom.io', 'zendesk.com', 'freshdesk.com',
            'salesforce.com', 'hubspot.com', 'marketo.com',
            
            # Error tracking and monitoring
            'sentry.io', 'rollbar.com', 'newrelic.com', 'datadog.com',
            
            # Cookie and privacy services
            'cookiebot.com', 'onetrust.com', 'privacymanager.io',
            'cookielaw.org', 'iubenda.com', 'termly.io',
            
            # Ad services
            'doubleclick.net', 'adsense.com', 'adroll.com',
            'adnxs.com', 'taboola.com', 'outbrain.com',
            
            # CDN and hosting services
            'cloudflare.com', 'akamai.com', 'fastly.com',
            'amazonaws.com', 'heroku.com', 'netlify.com',
            
            # Payment services
            'stripe.com', 'paypal.com', 'square.com',
            
            # Common noreply domains
            'noreply.com', 'no-reply.com', 'donotreply.com'
        }
        
        # Common prefixes that indicate business emails
        self.business_prefixes = {
            'info', 'contact', 'sales', 'support', 'admin', 'office',
            'hello', 'enquiry', 'inquiry', 'business', 'service',
            'mail', 'reception', 'booking', 'reservation', 'export',
            'order', 'purchase', 'commercial', 'wholesale', 'retail',
            'marketing', 'press', 'media', 'hr', 'careers', 'jobs',
            'webmaster', 'postmaster', 'hostmaster', 'billing'
        }

    def _deduplicate_parsing_errors(self, emails: Set[str]) -> Set[str]:
        """
        Post-processes a set of emails to remove likely parsing errors.
        For example, if both 'info@domain.com' and '123info@domain.com' are found,
        it removes the latter as it's likely an error.
        """
        if not emails:
            return emails

        # Create a copy to iterate over while modifying the original set
        emails_to_check = list(emails)
        to_remove = set()

        for email1 in emails_to_check:
            if email1 in to_remove:
                continue

            try:
                local1, domain1 = email1.split('@', 1)
            except ValueError:
                continue

            for email2 in emails_to_check:
                if email1 == email2:
                    continue
                
                try:
                    local2, domain2 = email2.split('@', 1)
                except ValueError:
                    continue

                # Check if they are for the same domain
                if domain1 == domain2:
                    # Is local1 a superset of local2 with a numeric prefix?
                    if len(local1) > len(local2) and local1.endswith(local2):
                        prefix = local1[:-len(local2)]
                        if prefix.isdigit():
                            self.logger.debug(f"Found parsing error. Removing '{email1}' because correct version '{email2}' also exists.")
                            to_remove.add(email1)
        
        return emails - to_remove
        
    def extract_emails_from_text(self, text: str) -> Set[str]:
        emails = set()
        
        for pattern in self.compiled_patterns:
            matches = pattern.findall(text)
            for match in matches:
                # Clean up the email
                email = self.clean_email(match)
                if email and self.is_valid_email(email):
                    emails.add(email.lower())
                    
        return emails
        
    def extract_emails_from_html(self, html: str, base_url: str = None) -> Set[str]:
        emails = set()
        soup = BeautifulSoup(html, 'lxml')
        
        # Extract from mailto links
        mailto_links = soup.find_all('a', href=re.compile(r'^mailto:', re.I))
        for link in mailto_links:
            href = link.get('href', '')
            email_match = re.search(r'mailto:([^?&]+)', href, re.I)
            if email_match:
                email = self.clean_email(email_match.group(1))
                if email and self.is_valid_email(email):
                    emails.add(email.lower())
        
        # Extract from text content, using a separator to prevent words from merging
        text_content = soup.get_text(separator=' ')
        text_emails = self.extract_emails_from_text(text_content)
        emails.update(text_emails)
        
        # Extract from specific elements that commonly contain emails
        email_selectors = [
            '.contact-email', '.email', '.contact-info', '.footer-contact',
            '[class*="email"]', '[class*="contact"]', '[id*="email"]',
            '[id*="contact"]', 'address', '.footer', '.company-email',
            '.business-email', '.support-email', '.info-email',
            '[data-type="email"]', '[data-field="email"]',
            '.contact-details', '.business-details', '.company-details'
        ]
        
        for selector in email_selectors:
            elements = soup.select(selector)
            for element in elements:
                element_text = element.get_text(separator=' ')
                element_emails = self.extract_emails_from_text(element_text)
                emails.update(element_emails)
                
        # Post-process the final set to remove likely parsing errors
        final_emails = self._deduplicate_parsing_errors(emails)
        
        return final_emails
        
    def clean_email(self, email: str) -> Optional[str]:
        """
        Clean and normalize email address.
        
        Args:
            email: Raw email string
            
        Returns:
            Cleaned email or None if invalid
        """
        if not email:
            return None
            
        # Aggressively remove anything that looks like an HTML tag or is inside angle brackets
        email = re.sub(r'<[^>]+>', '', email)

        # Remove whitespace
        email = email.strip()
        
        # Handle obfuscated emails
        email = email.replace('[at]', '@').replace('(at)', '@')
        email = email.replace('[dot]', '.').replace('(dot)', '.')
        email = re.sub(r'\s+', '', email)  # Remove all whitespace
        
        # Remove common unwanted characters
        email = email.strip('.,;:!?()[]{}"\'-')
        
        return email if email else None
        
    def is_valid_email(self, email: str) -> bool:
        """
        Validate email address.
        
        Args:
            email: Email to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not email or '@' not in email:
            return False
            
        # Use validators library for basic validation
        if not validators.email(email):
            return False
            
        # Extract domain
        domain = email.split('@')[1].lower()
        
        # Exclude certain domains  
        if domain in self.excluded_domains:
            return False
            
        # Must have valid TLD
        if '.' not in domain or len(domain.split('.')[-1]) < 2:
            return False
            
        return True
        
    def score_email_business_relevance(self, email: str) -> float:
        if not email:
            return 0.0
            
        local_part, domain = email.split('@')
        local_part = local_part.lower()
        domain = domain.lower()
        
        score = 0.5  # Base score
        
        # Business-like prefixes increase score
        for prefix in self.business_prefixes:
            if local_part.startswith(prefix):
                score += 0.3
                break
                
        # Personal email domains decrease score
        if domain in self.business_domains:
            score -= 0.2
            
        # Company domain (not free email) increases score
        elif domain not in self.business_domains:
            score += 0.2
            
        # Avoid obvious test/fake emails
        if any(word in local_part for word in ['test', 'fake', 'example', 'noreply', 'no-reply']):
            score -= 0.4
            
        # Prefer shorter, professional local parts
        if len(local_part) <= 10:
            score += 0.1
        elif len(local_part) > 20:
            score -= 0.1
            
        return max(0.0, min(1.0, score))
        
    def filter_business_emails(self, emails: Set[str], min_score: float = 0.3) -> List[str]:
        """
        Filter emails to keep only business-relevant ones.
        
        Args:
            emails: Set of emails to filter
            min_score: Minimum business relevance score
            
        Returns:
            List of filtered emails sorted by relevance score
        """
        scored_emails = []
        
        for email in emails:
            score = self.score_email_business_relevance(email)
            if score >= min_score:
                scored_emails.append((email, score))
                
        # Sort by score (highest first)
        scored_emails.sort(key=lambda x: x[1], reverse=True)
        
        return [email for email, score in scored_emails]
        
    def extract_and_filter_emails(self, content: str, content_type: str = 'html', 
                                 base_url: str = None, min_score: float = 0.3) -> List[str]:
        """
        Main method to extract and filter emails from content.
        
        Args:
            content: Content to extract from (HTML or text)
            content_type: 'html' or 'text'
            base_url: Base URL for HTML content
            min_score: Minimum business relevance score
            
        Returns:
            List of filtered business emails
        """
        if content_type.lower() == 'html':
            emails = self.extract_emails_from_html(content, base_url)
        else:
            emails = self.extract_emails_from_text(content)
            
        return self.filter_business_emails(emails, min_score)

