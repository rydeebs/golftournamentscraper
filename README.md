# Golf Tournament Scraper

A simple Streamlit web application for scraping golf tournament data from state golf association websites.

## Overview

The Golf Tournament Scraper helps golf operations founders extract tournament data (especially one-day tournaments or qualifying rounds) quickly from any state golf association website. The app automatically identifies and categorizes tournaments, detects qualifying events, and provides easy filtering and export options.

## Features

- Scrapes tournament names, dates, golf course information, and locations
- Automatically categorizes tournaments (Championship, Qualifier, One-Day)
- Identifies and extracts qualifying rounds as standalone tournaments
- Exports data to CSV
- User-friendly filtering options
- Real-time progress feedback during scraping

## Super Minimal Requirements

This application uses only the most essential Python libraries to ensure compatibility with most environments:

- streamlit==1.24.0
- requests==2.28.0
- beautifulsoup4==4.11.0

## Installation

1. Clone or download this repository:
```bash
git clone https://github.com/yourusername/golf-tournament-scraper.git
cd golf-tournament-scraper
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the application:
```bash
streamlit run app.py
```

## Quick Start

1. Enter the URL of a golf association tournament page (e.g., https://www.fsga.org/TournamentResults)
2. Click "Scrape Tournaments"
3. Use the filters to find specific tournament types or qualifying events
4. Download the data as CSV

## Troubleshooting

If you encounter installation issues, try the following:

1. Use the provided setup script:
```bash
chmod +x setup.sh
./setup.sh
```

2. Ensure you have Python 3.7+ installed
3. Try installing one dependency at a time:
```bash
pip install streamlit==1.24.0
pip install requests==2.28.0
pip install beautifulsoup4==4.11.0
```

## Customization

The scraper is designed to work with many golf association websites, but some customization might be needed for specific sites. Look for these sections in the code:

- `scrape_tournaments`: Main function that identifies tournament elements
- `scrape_detail_page`: Function that extracts information from detail pages
- `determine_tournament_type`: Logic to categorize tournaments
- `parse_date`: Function that handles various date formats

## License

This project is available under the MIT License.

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues if you have suggestions for improvements.
