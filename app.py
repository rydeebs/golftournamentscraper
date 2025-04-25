import streamlit as st
import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
from datetime import datetime
import time
import base64
from urllib.parse import urljoin
import logging

# Optional: For JavaScript-rendered pages
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    selenium_available = True
except ImportError:
    selenium_available = False

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Function to download data as CSV
def get_table_download_link(df):
    """Generates a link allowing the data in a given pandas dataframe to be downloaded"""
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="tournament_data.csv">Download CSV file</a>'
    return href

# Function to determine tournament type based on name or description
def determine_tournament_type(name, description=""):
    """Logic to categorize tournament type"""
    name_lower = name.lower()
    desc_lower = description.lower() if description else ""
    combined = name_lower + " " + desc_lower
    
    if any(qualifier in combined for qualifier in ['qualifier', 'qualifying', 'q-school']):
        return "Qualifier"
    elif any(championship in combined for championship in ['championship', 'amateur', 'open', 'invitational']):
        return "Championship"
    elif any(one_day in combined for one_day in ['one-day', 'one day', '1-day', '1 day']):
        return "One-Day"
    else:
        # Default to Championship if it's not clearly a qualifier or one-day event
        return "Championship"

# Function to check if a tournament is a qualifier
def is_qualifier(name, description=""):
    """Check if the tournament is a qualifier"""
    name_lower = name.lower()
    desc_lower = description.lower() if description else ""
    combined = name_lower + " " + desc_lower
    
    return any(qualifier in combined for qualifier in ['qualifier', 'qualifying', 'q-school'])

# Function to get HTML content using requests or fallback to Selenium
def get_page_html(url, use_selenium=False):
    """Get HTML content from URL using requests or Selenium if needed"""
    if not use_selenium:
        try:
            response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
            response.raise_for_status()
            return response.text
        except (requests.RequestException, requests.Timeout) as e:
            logger.warning(f"Request failed: {e}")
            if selenium_available:
                logger.info("Falling back to Selenium")
                return get_page_html(url, use_selenium=True)
            else:
                st.error(f"Failed to fetch data from {url}. Selenium is not available as fallback.")
                return None
    else:
        if not selenium_available:
            st.error("Selenium is not available. Please install it to handle JavaScript-rendered pages.")
            return None
        
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        try:
            driver = webdriver.Chrome(options=options)
            driver.get(url)
            # Wait for the page to load
            time.sleep(3)
            html = driver.page_source
            driver.quit()
            return html
        except Exception as e:
            logger.error(f"Selenium error: {e}")
            return None

# Function to parse date string into a standardized format
def parse_date(date_string):
    """Parse various date formats into a standardized format"""
    date_string = date_string.strip()
    
    # Handle common date formats
    formats = [
        '%B %d, %Y',  # January 1, 2023
        '%b %d, %Y',  # Jan 1, 2023
        '%m/%d/%Y',   # 01/01/2023
        '%Y-%m-%d',   # 2023-01-01
        '%m-%d-%Y',   # 01-01-2023
        '%d %B %Y',   # 1 January 2023
        '%d %b %Y',   # 1 Jan 2023
    ]
    
    for date_format in formats:
        try:
            parsed_date = datetime.strptime(date_string, date_format)
            return parsed_date.strftime('%Y-%m-%d')  # Return in ISO format
        except ValueError:
            continue
    
    # Handle date ranges by taking the start date
    date_range_match = re.search(r'(\w+ \d+)\s*-\s*(\w+ \d+),?\s*(\d{4})', date_string)
    if date_range_match:
        start_date, end_date, year = date_range_match.groups()
        try:
            parsed_date = datetime.strptime(f"{start_date}, {year}", '%B %d, %Y')
            return parsed_date.strftime('%Y-%m-%d')
        except ValueError:
            try:
                parsed_date = datetime.strptime(f"{start_date}, {year}", '%b %d, %Y')
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                pass
    
    # If no standard format matches, return original
    logger.warning(f"Could not parse date: {date_string}")
    return date_string

# Function to extract location from a text string
def extract_location(text):
    """Extract city and state from location text"""
    # Look for patterns like "City, ST" or "City, State"
    location_match = re.search(r'([A-Za-z\s\.]+),\s*([A-Z]{2}|[A-Za-z\s]+)', text)
    if location_match:
        city, state = location_match.groups()
        return f"{city.strip()}, {state.strip()}"
    return text.strip()

# Function to scrape tournament details from detail page
def scrape_detail_page(detail_url, base_url):
    """Scrape additional information from tournament detail page"""
    full_url = urljoin(base_url, detail_url) if not detail_url.startswith(('http://', 'https://')) else detail_url
    logger.info(f"Scraping detail page: {full_url}")
    
    html = get_page_html(full_url)
    if not html:
        return {}
    
    soup = BeautifulSoup(html, 'html.parser')
    details = {}
    
    # Look for course name - This will be site-specific and may need adjustment
    course_element = soup.find('h2') or soup.find('h3') or soup.find(class_='venue')
    if course_element:
        details['Golf Course Name'] = course_element.text.strip()
    
    # Look for location - This will be site-specific
    location_element = soup.find(class_='location') or soup.find(string=re.compile(r'[A-Za-z\s]+,\s*[A-Z]{2}'))
    if location_element:
        details['Location'] = extract_location(location_element.text)
    
    # Look for more detailed description
    description_element = soup.find(class_='description') or soup.find(class_='tournament-description')
    if description_element:
        details['Description'] = description_element.text.strip()
    
    # Look for qualifying rounds if this is a championship
    qualifying_sections = []
    qualifier_elements = soup.find_all(string=re.compile(r'Qualify|Qualifier|Qualifying'))
    
    for element in qualifier_elements:
        parent = element.parent
        # Look for nearby elements that might contain qualifier info
        section = parent.find_next('div') or parent.find_next('p') or parent.find_next('table')
        if section:
            qualifying_sections.append(section)
    
    qualifiers = []
    for section in qualifying_sections:
        # Extract qualifier info (this is highly dependent on the site structure)
        # For example, look for date, location, course name
        qualifier_info = {
            'Name': None,
            'Date': None,
            'Golf Course Name': None,
            'Location': None
        }
        
        # Find date pattern in text
        date_match = re.search(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:st|nd|rd|th)?,? \d{4}|\d{1,2}/\d{1,2}/\d{4}', section.text)
        if date_match:
            qualifier_info['Date'] = parse_date(date_match.group(0))
        
        # Find location pattern
        location_match = re.search(r'([A-Za-z\s\.]+),\s*([A-Z]{2}|[A-Za-z\s]+)', section.text)
        if location_match:
            qualifier_info['Location'] = f"{location_match.group(1).strip()}, {location_match.group(2).strip()}"
        
        # If we found some qualifier info, add it to our list
        if qualifier_info['Date'] or qualifier_info['Location']:
            qualifiers.append(qualifier_info)
    
    details['Qualifiers'] = qualifiers
    return details

# Main function to scrape tournament list
def scrape_tournaments(url):
    """Main function to scrape tournament list from association website"""
    st.write(f"Scraping data from: {url}")
    
    # Get HTML content
    html = get_page_html(url)
    if not html:
        return pd.DataFrame()
    
    # Parse HTML
    soup = BeautifulSoup(html, 'html.parser')
    
    # Find tournament elements - This will be site-specific and may need adjustment
    # Common classes or IDs for tournament listings
    tournament_elements = (
        soup.find_all(class_=['tournament', 'event', 'tournament-item']) or 
        soup.find_all('tr') or 
        soup.find_all('div', class_=lambda c: c and ('event' in c.lower() or 'tournament' in c.lower()))
    )
    
    logger.info(f"Found {len(tournament_elements)} potential tournament elements")
    
    # Prepare data structure
    tournaments = []
    
    # Loop through tournament elements
    for element in tournament_elements:
        # Extract basic information - This will be site-specific and may need adjustment
        tournament_name_element = element.find('h3') or element.find('h4') or element.find('a') or element.find('td')
        
        if not tournament_name_element:
            continue
            
        tournament_name = tournament_name_element.text.strip()
        
        # Skip if doesn't look like a tournament name
        if len(tournament_name) < 5 or tournament_name.lower() in ['date', 'tournament', 'event']:
            continue
            
        logger.info(f"Processing tournament: {tournament_name}")
        
        # Find detail link
        detail_link = None
        link_element = element.find('a')
        if link_element and 'href' in link_element.attrs:
            detail_link = link_element['href']
            
        # Extract date - look for date patterns
        date_element = element.find(string=re.compile(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:st|nd|rd|th)?,? \d{4}|\d{1,2}/\d{1,2}/\d{4}'))
        date = None
        if date_element:
            date = parse_date(date_element)
        
        # Initialize tournament data
        tournament_data = {
            'Tournament Name': tournament_name,
            'Date': date,
            'Golf Course Name': None,
            'Location': None,
            'Tournament Type': determine_tournament_type(tournament_name),
            'Is Qualifier': is_qualifier(tournament_name),
            'Detail URL': detail_link
        }
        
        # If we have a detail link, scrape additional information
        if detail_link:
            detail_data = scrape_detail_page(detail_link, url)
            
            # Update tournament data with detail information
            for key, value in detail_data.items():
                if key != 'Qualifiers' and value:
                    tournament_data[key] = value
                    
            # Process qualifiers if found
            if 'Qualifiers' in detail_data and detail_data['Qualifiers']:
                for qualifier in detail_data['Qualifiers']:
                    qualifier_data = {
                        'Tournament Name': f"{tournament_name} - Qualifier",
                        'Date': qualifier.get('Date', tournament_data['Date']),
                        'Golf Course Name': qualifier.get('Golf Course Name'),
                        'Location': qualifier.get('Location'),
                        'Tournament Type': 'Qualifier',
                        'Is Qualifier': True,
                        'Detail URL': detail_link  # Use main tournament detail link
                    }
                    tournaments.append(qualifier_data)
        
        # Add tournament to list
        tournaments.append(tournament_data)
    
    # Convert to DataFrame
    df = pd.DataFrame(tournaments)
    
    # Clean up and fill missing values
    if not df.empty:
        # Drop duplicates
        df.drop_duplicates(subset=['Tournament Name', 'Date'], inplace=True)
        
        # Fill missing values
        df['Tournament Type'] = df['Tournament Type'].fillna('Championship')
        df['Is Qualifier'] = df['Is Qualifier'].fillna(False)
        
        # Reorder columns
        column_order = [
            'Tournament Name', 'Date', 'Golf Course Name', 'Location', 
            'Tournament Type', 'Is Qualifier', 'Detail URL'
        ]
        df = df[column_order]
    
    logger.info(f"Scraped {len(df)} tournaments")
    return df

# Streamlit UI
def main():
    st.title("Golf Tournament Data Scraper")
    
    st.markdown("""
    This app scrapes tournament data from state golf association websites.
    Enter the URL of a tournament results page below.
    """)
    
    # Input for URL
    url = st.text_input(
        "Enter Tournament Page URL",
        value="https://www.fsga.org/TournamentResults"
    )
    
    # Checkbox for using Selenium
    use_selenium = False
    if selenium_available:
        use_selenium = st.checkbox("Use Selenium for JavaScript-rendered pages", value=False)
    else:
        st.warning("Selenium is not available. For JavaScript-rendered pages, install selenium package.")
    
    # Button to start scraping
    if st.button("Scrape Tournaments"):
        if url:
            with st.spinner('Scraping data...'):
                # Perform scraping
                df = scrape_tournaments(url)
                
                if df.empty:
                    st.error("No tournament data found. The website structure might not be supported.")
                else:
                    # Display results
                    st.success(f"Found {len(df)} tournaments")
                    st.dataframe(df)
                    
                    # Provide download link
                    st.markdown(get_table_download_link(df), unsafe_allow_html=True)
                    
                    # Display some stats
                    st.subheader("Tournament Statistics")
                    st.write(f"Total Tournaments: {len(df)}")
                    
                    types = df['Tournament Type'].value_counts()
                    st.write("Tournament Types:")
                    st.write(types)
                    
                    qualifier_count = df['Is Qualifier'].sum()
                    st.write(f"Qualifying Events: {qualifier_count}")
        else:
            st.error("Please enter a valid URL")

if __name__ == "__main__":
    main()
