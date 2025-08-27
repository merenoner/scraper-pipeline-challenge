# Scraper Pipeline Challenge

A comprehensive Python project developed for collecting company information and email addresses from B2B portals using modern web scraping pipeline.

## 🎯 Project Purpose

This project is designed to automatically collect, process, and analyze company information from B2B (Business-to-Business) portals -**Europages** and **WLW** for now-. It specifically focuses on obtaining the following data:

- Company names and profile links
- Contact information (email addresses)
- Company addresses and country information
- Company websites

## 🏗️ Project Architecture

### Core Components

```
scraper-pipeline-challenge/
├── main.py                 # Main application entry point
├── config/
│   └── configuration.json  # Portal configurations
├── scraper/               # Web scraping modules
│   ├── selenium_scraper.py    # Selenium-based scraper
│   ├── requests_scraper.py    # Requests-based scraper
│   └── selenium_handler.py    # Selenium driver management
├── processor/             # Data processing modules
│   ├── data_processor.py      # Data cleaning and processing
│   └── email_extractor.py     # Email extraction and filtering
└── data/                  # Output files
    ├── links_<sector>.csv     # Company profile links
    ├── emails_<sector>.csv    # Email lists
    ├── detailed_<sector>.csv  # Detailed company information
    └── summary_<sector>.log   # Scraping summary report
```

### Scraper Engine Factory Pattern

The project supports different scraping engines using the **Factory Pattern**:

- **Selenium Engine**: Full browser simulation for JavaScript-heavy sites, both portals use this engine.
- **Requests Engine**: Fast and lightweight HTTP requests

## 🚀 Installation and Usage

### Requirements

Install dependencies from requirements.txt:

```bash
pip install -r requirements.txt
```

Or install manually:
```bash
pip install selenium requests beautifulsoup4 pandas validators lxml
```

Make sure Chrome WebDriver is installed on your system.

### Basic Usage

```bash
python main.py --portal europages --sector winery --max_pages 5
```

### Parameters

- `--portal`: Portal to scrape (`europages`, `wlw`)
- `--sector`: Target sector (e.g., `winery`, `cosmetics`)
- `--max_pages`: Maximum number of pages
- `--no-headless`: Run browser in visible mode (optional)


## ⚙️ Portal Configuration

### Available Portals

#### 1. Europages
- **Engine**: Selenium
- **Base URL**: `https://www.europages.co.uk`
- **Features**: Static pagination, standard CSS selectors

#### 2. WLW (Wer Liefert Was)
- **Engine**: Selenium  
- **Base URL**: `https://www.wlw.de/`
- **Features**: Dynamic pagination (click-based), interactive elements

### Adding New Portals

To add a new portal to the `config/configuration.json` file:

```json
{
  "portals": {
    "new_portal": {
      "engine": "selenium",  // or "requests"
      "base_url": "https://example.com",
      "search_path_template": "/search/{sector}/page-{page}",
      "selectors": {
        "company_profiles": "a.company-link",
        "next_page": "a.next-button",
        "company_name": "h1.company-title",
        "email_containers": "a[href^='mailto:']",
        "website_links": "a.website-btn",
        "company_address": ".address",
        "country": ".country"
      },
      "max_pages": 50,
      "delay_between_requests": 2
    }
  }
}
```

### Selector Configuration

The following CSS selectors must be defined for each portal:

- `company_profiles`: Elements containing company profile links
- `next_page`: Next page button
- `company_name`: Company name element
- `email_containers`: Elements containing emails
- `website_links`: Company website links
- `company_address`: Address information
- `country`: Country information

## 📊 Output Files

### 1. `links_<sector>.csv`
List of found company profile links:
```csv
profile_url
https://www.europages.co.uk/company1
https://www.europages.co.uk/company2
```

### 2. `emails_<sector>.csv`
Company information with email addresses:
```csv
Name,Country,Email
Company A,Germany,info@companya.com
Company B,France,contact@companyb.fr
```

### 3. `detailed_<sector>.csv`
Main dataset containing all details:
```csv
Name,Country,Address,Website,Email,Profile_URL
Company A,Germany,Berlin Street 123,https://companya.com,info@companya.com,https://portal.com/company-a
```

### 4. `summary_<sector>.log`
Scraping process summary report:
```
--- Scraping Summary ---
Start Time: 2025-01-20 10:00:00
End Time:   2025-01-20 10:15:30
Duration:   0:15:30

Scraped 5 pages and found 120 company profiles.

--- Final Statistics ---
Total companies with at least one email found: 85 / 120

Country Frequency:
- Germany: 45
- France: 30
- Italy: 25
```

## 🔧 Advanced Features

### Multi-Threading Support

Selenium scraper supports parallel processing to improve performance:
- Company detail extraction with 7 parallel threads
- Automatic thread management and cleanup

### Intelligent Email Extraction

Email Extractor module offers advanced filtering features:

- **Obfuscated email support**: `[at]`, `(at)`, `[dot]` formats
- **Business email scoring**: Priority for business emails
- **Spam domain filtering**: Filters test/fake domains
- **Duplicate detection**: Cleans parsing errors

### Error Handling & Recovery

- Robust exception handling at every level
- Automatic cookie banner acceptance
- Timeout management
- Automatic retry mechanisms

### Data Quality Assurance

- **Data cleaning**: Company name normalization
- **Country standardization**: ISO country codes → full names
- **Duplicate removal**: Smart duplicate detection
- **Validation**: Email and URL validation

## 🔍 Technical Details

### Portal-Specific Optimizations

#### WLW Portal
- Cookie consent handling
- Button-to-link transformation logic
- Click-based pagination
- Dynamic content waiting

#### Europages Portal
- Standard link extraction
- Static pagination
- Direct URL navigation


## 🛠️ Troubleshooting

### Common Issues

1. **Chrome Driver Issues**
   ```bash
   # Update Chrome driver
   pip install --upgrade selenium
   ```

2. **Timeout Errors**
   - Reduce `max_pages` value
   - Check your internet connection

3. **Captcha/Bot Detection**
   - Debug with `--no-headless`
   - Increase delay values

4. **Empty Results**
   - Check selectors
   - Verify portal's URL format


## 🎯 Future Plans

- [ ] API-based data export
- [ ] Real-time monitoring dashboard
- [ ] Machine learning powered email quality scoring
- [ ] Additional B2B portal integrations
- [ ] Cloud deployment support
- [ ] Database integration options

