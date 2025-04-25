import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
import csv
import io
import base64
from urllib.parse import urljoin
import logging
import time
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
    except (requests.RequestException, requests.Timeout) as e:
        logger.warning(f"Request failed: {e}")
        st.error(f"Failed to fetch data from {url}. Error: {str(e)}")
        return None

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
    
    # Look for course name
    course_elements = [
        soup.find('h2'),
        soup.find('h3'),
        soup.find(class_='venue'),
        soup.find(string=re.compile(r'(?:Golf Club|Country Club|Course)')),
        soup.find('p', string=re.compile(r'(?:Golf Club|Country Club|Course)'))
    ]
    
    for element in course_elements:
        if element and element.text.strip():
            details['Golf Course Name'] = element.text.strip()
            break
    
    # Look for location
    location_elements = [
        soup.find(class_='location'),
        soup.find(string=re.compile(r'[A-Za-z\s]+,\s*[A-Z]{2}')),
        soup.find('p', string=re.compile(r'[A-Za-z\s]+,\s*[A-Z]{2}'))
    ]
    
    for element in location_elements:
        if element and element.text.strip():
            details['Location'] = extract_location(element.text)
            break
    
    # Look for more detailed description
    description_elements = [
        soup.find(class_='description'),
        soup.find(class_='tournament-description'),
        soup.find('div', id=re.compile(r'description')),
        soup.find('p', class_=re.compile(r'desc'))
    ]
    
    for element in description_elements:
        if element and element.text.strip():
            details['Description'] = element.text.strip()
            break
    
    # Look for qualifying rounds
    qualifier_sections = []
    qualifier_patterns = ['Qualify', 'Qualifier', 'Qualifying']
    
    for pattern in qualifier_patterns:
        elements = soup.find_all(string=re.compile(pattern, re.IGNORECASE))
        for element in elements:
            parent = element.parent
            # Look for nearby elements that might contain qualifier info
            section = parent.find_next('div') or parent.find_next('p') or parent.find_next('table')
            if section:
                qualifier_sections.append(section)
    
    qualifiers = []
    for section in qualifier_sections:
        # Extract qualifier info
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
def scrape_tournaments(url, progress_callback=None):
    """Main function to scrape tournament list from association website"""
    if progress_callback:
        progress_callback(f"Starting to scrape: {url}")
    
    # Get HTML content
    html = get_page_html(url)
    if not html:
        if progress_callback:
            progress_callback("Failed to get HTML content")
        return []
    
    if progress_callback:
        progress_callback("Parsing HTML and looking for tournaments...")
    
    # Parse HTML
    soup = BeautifulSoup(html, 'html.parser')
    
    # Find potential tournament elements using different strategies
    tournament_elements = []
    
    # Strategy 1: Look for tables
    tables = soup.find_all('table')
    if tables and progress_callback:
        progress_callback(f"Found {len(tables)} tables. Analyzing...")
    
    for table in tables:
        rows = table.find_all('tr')
        if len(rows) > 1:  # Skip tables with only header row
            tournament_elements.extend(rows[1:])  # Skip header row
    
    # Strategy 2: Look for lists if no tables or tables didn't yield enough
    if len(tournament_elements) < 3:
        lists = soup.find_all(['ul', 'ol'])
        if lists and progress_callback:
            progress_callback(f"Found {len(lists)} lists. Analyzing...")
        
        for list_element in lists:
            items = list_element.find_all('li')
            if len(items) > 3:  # Only consider lists with several items
                tournament_elements.extend(items)
    
    # Strategy 3: Look for specific div patterns if still not enough
    if len(tournament_elements) < 3:
        if progress_callback:
            progress_callback("Looking for div patterns...")
        
        # Common patterns in tournament listings
        patterns = [
            ['div', {'class': lambda c: c and ('tournament' in c.lower() or 'event' in c.lower())}],
            ['div', {'class': lambda c: c and ('item' in c.lower() or 'result' in c.lower())}],
            ['article', {}],
            ['section', {'class': lambda c: c and ('list' in c.lower() or 'results' in c.lower())}]
        ]
        
        for selector, attrs in patterns:
            elements = soup.find_all(selector, attrs)
            if elements:
                tournament_elements.extend(elements)
                if len(tournament_elements) > 10:
                    break
    
    if progress_callback:
        progress_callback(f"Found {len(tournament_elements)} potential tournament elements")
    
    logger.info(f"Found {len(tournament_elements)} potential tournament elements")
    
    # Prepare data structure
    tournaments = []
    
    # Loop through tournament elements
    for i, element in enumerate(tournament_elements):
        if progress_callback and i % 5 == 0:
            progress_callback(f"Processing element {i+1} of {len(tournament_elements)}...")
        
        # Extract basic information
        tournament_name_element = (
            element.find('h3') or 
            element.find('h4') or 
            element.find('a') or 
            element.find('td') or
            element.find(class_=lambda c: c and ('title' in str(c).lower()))
        )
        
        if not tournament_name_element:
            # If no specific element found, try using the element itself
            if element.name in ['li', 'td', 'div'] and element.text.strip():
                tournament_name = element.text.strip()
            else:
                continue
        else:
            tournament_name = tournament_name_element.text.strip()
        
        # Skip if doesn't look like a tournament name
        if len(tournament_name) < 5 or tournament_name.lower() in ['date', 'tournament', 'event', 'name']:
            continue
            
        logger.info(f"Processing tournament: {tournament_name}")
        
        # Find detail link
        detail_link = None
        link_element = element.find('a')
        if link_element and 'href' in link_element.attrs:
            detail_link = link_element['href']
            
        # Extract date - look for date patterns
        date_text = None
        
        # Look for date in the element text or in a specific child element
        date_element = element.find(string=re.compile(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:st|nd|rd|th)?,? \d{4}|\d{1,2}/\d{1,2}/\d{4}'))
        if date_element:
            date_text = date_element.strip()
        else:
            # Try to find date in a column or specific element
            date_cell = element.find('td', class_=lambda c: c and 'date' in str(c).lower())
            if date_cell:
                date_text = date_cell.text.strip()
        
        date = None
        if date_text:
            date = parse_date(date_text)
        
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
        
        # Extract location and course directly from the listing if possible
        location_element = element.find(string=re.compile(r'[A-Za-z\s]+,\s*[A-Z]{2}'))
        if location_element:
            tournament_data['Location'] = extract_location(location_element)
        
        course_element = element.find(class_=['course', 'venue', 'location'])
        if course_element:
            tournament_data['Golf Course Name'] = course_element.text.strip()
        
        # If we have a detail link, scrape additional information
        if detail_link and len(tournaments) < 10:  # Limit detail page scraping to avoid rate limiting
            if progress_callback:
                progress_callback(f"Checking detail page for: {tournament_name}")
                
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
    
    logger.info(f"Scraped {len(tournaments)} tournaments")
    return tournaments

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
    
    # Add more UI options in a sidebar
    st.sidebar.header("Scraping Options")
    
    # Simple mode toggle
    simple_mode = st.sidebar.checkbox("Quick Mode (Faster Results)", value=True)
    
    # Debug mode
    debug_mode = st.sidebar.checkbox("Debug Mode", value=False)
    if debug_mode:
        st.sidebar.info("Debug mode will show more detailed information during scraping")
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Button to start scraping
    if st.button("Scrape Tournaments"):
        if url:
            with st.spinner('Scraping data...'):
                try:
                    # Show the URL being scraped
                    st.info(f"Scraping data from: {url}")
                    
                    # Check connection first
                    try:
                        requests.head(url, timeout=5)
                    except requests.RequestException as e:
                        st.error(f"Could not connect to {url}: {str(e)}")
                        return
                    
                    # Add a progress placeholder
                    progress_placeholder = st.empty()
                    progress_placeholder.text("Fetching main page...")
                    
                    # Perform scraping with progress updates
                    def progress_callback(message):
                        progress_placeholder.text(message)
                    
                    # Perform scraping
                    tournaments = scrape_tournaments(url, progress_callback=progress_callback)
                    
                    # Clear progress message
                    progress_placeholder.empty()
                    
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
                        
                        # Convert to list of dicts for display
                        tournament_data = []
                        for t in filtered_tournaments:
                            tournament_data.append({
                                'Tournament Name': t.get('Tournament Name', ''),
                                'Date': t.get('Date', ''),
                                'Golf Course Name': t.get('Golf Course Name', ''),
                                'Location': t.get('Location', ''),
                                'Tournament Type': t.get('Tournament Type', ''),
                                'Is Qualifier': t.get('Is Qualifier', False),
                                'Detail URL': t.get('Detail URL', '')
                            })
                        
                        # Show filtered dataframe
                        st.subheader("Tournament Data")
                        # Convert to simpler display format
                        display_data = []
                        for t in tournament_data:
                            display_data.append({
                                'Name': t['Tournament Name'],
                                'Date': t['Date'] or 'N/A',
                                'Course': t['Golf Course Name'] or 'N/A',
                                'Location': t['Location'] or 'N/A',
                                'Type': t['Tournament Type'],
                                'Qualifier': 'Yes' if t['Is Qualifier'] else 'No'
                            })
                            
                        st.write(display_data)
                        
                        # Provide download link
                        st.markdown(get_table_download_link(tournament_data), unsafe_allow_html=True)
                        
                        # Display some stats
                        st.subheader("Tournament Statistics")
                        st.write(f"Total Tournaments: {len(tournaments)}")
                        
                        # Count by type and qualifier status
                        type_counts = {}
                        for t in tournaments:
                            t_type = t.get('Tournament Type', 'Unknown')
                            if t_type in type_counts:
                                type_counts[t_type] += 1
                            else:
                                type_counts[t_type] = 1
                        
                        qualifier_count = sum(1 for t in tournaments if t.get('Is Qualifier'))
                        
                        # Display stats in columns
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write("Tournament Types:")
                            for t_type, count in type_counts.items():
                                st.write(f"- {t_type}: {count}")
                        
                        with col2:
                            st.write(f"Qualifying Events: {qualifier_count}")
                            
                            # Count tournaments by month if date is available
                            months = {}
                            for t in tournaments:
                                if t.get('Date') and '-' in t.get('Date', ''):
                                    try:
                                        month_num = int(t['Date'].split('-')[1])
                                        month_name = datetime(2000, month_num, 1).strftime('%B')
                                        if month_name in months:
                                            months[month_name] += 1
                                        else:
                                            months[month_name] = 1
                                    except (ValueError, IndexError):
                                        pass
                            
                            if months:
                                st.write("Tournaments by Month:")
                                for month, count in months.items():
                                    st.write(f"- {month}: {count}")
                except Exception as e:
                    st.error(f"An error occurred during scraping: {str(e)}")
                    if debug_mode:
                        st.exception(e)
        else:
            st.error("Please enter a valid URL")
    
    # Add helpful instructions
    with st.expander("Tips for Better Results"):
        st.markdown("""
        - For best results, use URLs that point directly to tournament listing pages
        - The "Quick Mode" option will avoid scraping detail pages, which is faster but might miss some information
        - The scraper works best with standard HTML tables or list structures
        - Some websites may require additional customization to work properly
        - If you encounter errors, try enabling Debug Mode to see more information
        """)
        
    # Add footer
    st.markdown("---")
    st.markdown("Golf Tournament Scraper | Created for golf operations founders")

if __name__ == "__main__":
    main()
