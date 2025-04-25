import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
import csv
import io
import base64
from urllib.parse import urljoin
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Function to download data as CSV
def get_table_download_link(data):
    """Generates a link allowing the data to be downloaded as CSV"""
    csv_string = io.StringIO()
    writer = csv.writer(csv_string)
    
    # Write header
    if data and len(data) > 0:
        writer.writerow(data[0].keys())
        # Write rows
        for item in data:
            writer.writerow(item.values())
    
    csv_string = csv_string.getvalue()
    b64 = base64.b64encode(csv_string.encode()).decode()
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

# Function to parse date string into a standardized format
def parse_date(date_string):
    """Parse various date formats into a standardized format"""
    if not date_string:
        return None
        
    date_string = date_string.strip()
    
    # Handle common date formats
    date_formats = [
        '%B %d, %Y',  # January 1, 2023
        '%b %d, %Y',  # Jan 1, 2023
        '%m/%d/%Y',   # 01/01/2023
        '%Y-%m-%d',   # 2023-01-01
        '%m-%d-%Y',   # 01-01-2023
        '%d %B %Y',   # 1 January 2023
        '%d %b %Y',   # 1 Jan 2023
    ]
    
    for date_format in date_formats:
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
    if not text:
        return None
        
    # Look for patterns like "City, ST" or "City, State"
    location_match = re.search(r'([A-Za-z\s\.]+),\s*([A-Z]{2}|[A-Za-z\s]+)', text)
    if location_match:
        city, state = location_match.groups()
        return f"{city.strip()}, {state.strip()}"
    return text.strip()

# Function to get HTML content using requests
def get_page_html(url):
    """Get HTML content from URL using requests"""
    try:
        # Add common browser headers to avoid being blocked
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        return response.text
    except Exception as e:
        st.error(f"Failed to fetch data from {url}. Error: {str(e)}")
        return None

# Main function to scrape tournaments
def scrape_tournaments(url):
    """Main function to scrape tournament data from a URL"""
    # Get HTML content
    html = get_page_html(url)
    if not html:
        return []
        
    # Parse HTML
    soup = BeautifulSoup(html, 'html.parser')
    
    # Find potential tournament elements
    tournament_elements = []
    
    # Strategy 1: Look for tables
    tables = soup.find_all('table')
    for table in tables:
        rows = table.find_all('tr')
        if len(rows) > 1:  # Skip tables with only header row
            tournament_elements.extend(rows[1:])  # Skip header row
    
    # Strategy 2: Look for lists if no tables
    if len(tournament_elements) < 3:
        lists = soup.find_all(['ul', 'ol'])
        for list_element in lists:
            items = list_element.find_all('li')
            if len(items) > 2:  # Only consider lists with several items
                tournament_elements.extend(items)
    
    # Strategy 3: Look for specific div patterns if still not enough
    if len(tournament_elements) < 3:
        # Common patterns in tournament listings
        patterns = [
            ['div', {'class': lambda c: c and ('tournament' in c.lower() or 'event' in c.lower())}],
            ['div', {'class': lambda c: c and ('item' in c.lower() or 'result' in c.lower())}],
            ['article', {}],
            ['div', {'class': lambda c: c and ('list' in c.lower() or 'results' in c.lower())}]
        ]
        
        for selector, attrs in patterns:
            elements = soup.find_all(selector, attrs)
            if elements:
                tournament_elements.extend(elements)
                if len(tournament_elements) > 10:
                    break
    
    # Prepare data structure
    tournaments = []
    
    # Loop through tournament elements
    for element in tournament_elements:
        # Extract basic information
        tournament_name_element = (
            element.find('h3') or 
            element.find('h4') or 
            element.find('a') or 
            element.find('td') or
            element
        )
        
        if not tournament_name_element:
            continue
            
        tournament_name = tournament_name_element.get_text().strip()
        
        # Skip if doesn't look like a tournament name
        if len(tournament_name) < 5 or tournament_name.lower() in ['date', 'tournament', 'event', 'name']:
            continue
        
        # Find detail link
        detail_link = None
        link_element = element.find('a')
        if link_element and 'href' in link_element.attrs:
            detail_link = link_element['href']
            # Make sure URL is absolute
            if not detail_link.startswith(('http://', 'https://')):
                detail_link = urljoin(url, detail_link)
        
        # Extract date - look for date patterns
        date_match = re.search(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:st|nd|rd|th)?,? \d{4}|\d{1,2}/\d{1,2}/\d{4}', element.get_text())
        date = None
        if date_match:
            date = parse_date(date_match.group(0))
        
        # Extract location - look for location patterns
        location = None
        location_match = re.search(r'([A-Za-z\s\.]+),\s*([A-Z]{2})', element.get_text())
        if location_match:
            location = f"{location_match.group(1).strip()}, {location_match.group(2).strip()}"
            
        # Initialize tournament data
        tournament_data = {
            'Tournament Name': tournament_name,
            'Date': date,
            'Golf Course Name': None,
            'Location': location,
            'Tournament Type': determine_tournament_type(tournament_name),
            'Is Qualifier': is_qualifier(tournament_name),
            'Detail URL': detail_link
        }
        
        # Add tournament to list
        tournaments.append(tournament_data)
    
    return tournaments

# Streamlit UI
def main():
    st.title("Golf Tournament Scraper")
    
    st.markdown("""
    This app scrapes tournament data from state golf association websites.
    
    **Instructions:**
    1. Enter the URL of a tournament results page
    2. Click "Scrape Tournaments"
    3. Filter and download the results
    """)
    
    # Input for URL
    url = st.text_input(
        "Enter Tournament Page URL",
        value="https://www.fsga.org/TournamentResults"
    )
    
    # Button to start scraping
    if st.button("Scrape Tournaments"):
        if url:
            with st.spinner('Scraping data...'):
                try:
                    # Perform scraping
                    tournaments = scrape_tournaments(url)
                    
                    if not tournaments:
                        st.error("No tournament data found. The website structure might not be supported.")
                    else:
                        # Display results
                        st.success(f"Found {len(tournaments)} tournaments")
                        
                        # Add filter options
                        st.subheader("Filter Results")
                        tournament_types = ["All"] + list(set(t['Tournament Type'] for t in tournaments if t.get('Tournament Type')))
                        selected_type = st.selectbox("Tournament Type", tournament_types)
                        
                        show_qualifiers = st.checkbox("Show Only Qualifiers", value=False)
                        
                        # Apply filters
                        filtered_tournaments = tournaments.copy()
                        if selected_type != "All":
                            filtered_tournaments = [t for t in filtered_tournaments if t.get('Tournament Type') == selected_type]
                        
                        if show_qualifiers:
                            filtered_tournaments = [t for t in filtered_tournaments if t.get('Is Qualifier')]
                        
                        # Show filtered data
                        st.subheader("Tournament Data")
                        # Create a simpler display format
                        display_data = []
                        for t in filtered_tournaments:
                            display_data.append({
                                'Name': t.get('Tournament Name', ''),
                                'Date': t.get('Date', 'N/A'),
                                'Location': t.get('Location', 'N/A'),
                                'Type': t.get('Tournament Type', ''),
                                'Qualifier': 'Yes' if t.get('Is Qualifier') else 'No'
                            })
                            
                        st.write(display_data)
                        
                        # Provide download link
                        st.markdown(get_table_download_link(filtered_tournaments), unsafe_allow_html=True)
                        
                        # Display some stats
                        st.subheader("Tournament Statistics")
                        st.write(f"Total Tournaments: {len(tournaments)}")
                        
                        # Count by type
                        type_counts = {}
                        for t in tournaments:
                            t_type = t.get('Tournament Type', 'Unknown')
                            if t_type in type_counts:
                                type_counts[t_type] += 1
                            else:
                                type_counts[t_type] = 1
                                
                        st.write("Tournament Types:")
                        for t_type, count in type_counts.items():
                            st.write(f"- {t_type}: {count}")
                except Exception as e:
                    st.error(f"An error occurred during scraping: {str(e)}")
        else:
            st.error("Please enter a valid URL")
    
    # Add helpful instructions
    with st.expander("Tips for Better Results"):
        st.markdown("""
        - For best results, use URLs that point directly to tournament listing pages
        - The scraper works best with standard HTML tables or list structures
        - Some websites may require additional customization to work properly
        """)
        
    # Add footer
    st.markdown("---")
    st.markdown("Golf Tournament Scraper | Created for golf operations founders")

if __name__ == "__main__":
    main()
