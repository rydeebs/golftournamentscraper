# Golf Tournament Scraper

A Streamlit web application that scrapes golf tournament data from state golf association websites.

## Features

- Scrapes tournament names, dates, locations, and more from golf association websites
- Automatically categorizes tournaments (One-Day, Qualifier, Championship)
- Identifies qualifying rounds as standalone tournaments
- Exports data to CSV
- Supports both static and JavaScript-rendered pages (with Selenium)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/golf-tournament-scraper.git
cd golf-tournament-scraper
```

2. Install the required packages:
```bash
pip install -r requirements.txt
```

3. Optional: For JavaScript-rendered pages, uncomment and install Selenium dependencies in requirements.txt:
```bash
pip install selenium webdriver-manager
```

## Usage

Run the Streamlit app:
```bash
streamlit run app.py
```

Then open your browser and navigate to the displayed URL (typically http://localhost:8501).

### Using the App

1. Enter the URL of a golf association tournament page
2. Toggle the Selenium option if the page is JavaScript-rendered
3. Click "Scrape Tournaments" to start the scraping process
4. View the results and download as CSV if desired

## Example URLs

- Florida State Golf Association: https://www.fsga.org/TournamentResults
- Other state golf associations will have similar structures

## Customization

The scraper is designed to be adaptable to different websites, but may require adjustments for specific sites with unusual structures. Look for these sections in the code to customize:

- `scrape_tournaments`: Main function that identifies tournament elements
- `scrape_detail_page`: Function that scrapes additional information from detail pages
- `determine_tournament_type`: Logic to categorize tournaments
- `parse_date`: Function that handles various date formats

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
