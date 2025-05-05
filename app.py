import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
import csv
import io
import base64
from urllib.parse import urljoin
import logging
from datetime import datetime, timedelta
import time
import pandas as pd
import json
import hashlib
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Function to download data as CSV
def get_table_download_link(data, filename="tournament_data.csv"):
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
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">Download CSV file</a>'
    return href

# Function to download data as JSON
def get_json_download_link(data, filename="tournament_data.json"):
    """Generates a link allowing the data to be downloaded as JSON"""
    json_string = json.dumps(data, indent=2)
    b64 = base64.b64encode(json_string.encode()).decode()
    href = f'<a href="data:file/json;base64,{b64}" download="{filename}">Download JSON file</a>'
    return href

# Function to determine tournament type based on name or description
def determine_tournament_type(name, description=""):
    """Logic to categorize tournament type"""
    name_lower = name.lower()
    desc_lower = description.lower() if description else ""
    combined = name_lower + " " + desc_lower
    
    if any(qualifier in combined for qualifier in ['qualifier', 'qualifying', 'q-school', 'q school']):
        return "Qualifying Round"
    elif any(one_day in combined for one_day in ['one-day', 'one day', '1-day', '1 day', 'single day']):
        return "One-Day"
    elif any(amateur in combined for amateur in ['amateur', 'am-am', 'amam']):
        return "Amateur"
    elif any(junior in combined for junior in ['junior', 'jr', 'youth']):
        return "Junior"
    elif any(senior in combined for senior in ['senior', 'sr', 'mid-amateur', 'mid-am']):
        return "Senior"
    elif any(championship in combined for championship in ['championship', 'open', 'invitational', 'tournament', 'classic']):
        return "Championship"
    elif any(tour in combined for tour in ['tour', 'series', 'circuit']):
        return "Tour Event"
    else:
        # Default to Championship if it's not clearly another type
        return "Championship"

# Function to check if a tournament is a qualifier
def is_qualifier(name, description=""):
    """Check if the tournament is a qualifier"""
    name_lower = name.lower()
    desc_lower = description.lower() if description else ""
    combined = name_lower + " " + desc_lower
    
    return any(qualifier in combined for qualifier in ['qualifier', 'qualifying', 'q-school', 'q school'])

# Function to parse date string into a standardized format
def parse_date(date_string):
    """Parse various date formats into a standardized format"""
    if not date_string:
        return None
        
    date_string = date_string.strip()
    
    # Handle empty or invalid strings
    if date_string.lower() in ['tbd', 'tba', 'to be determined', 'to be announced']:
        return 'TBD'
    
    # Handle common date formats
    date_formats = [
        '%B %d, %Y',       # January 1, 2023
        '%b %d, %Y',       # Jan 1, 2023
        '%m/%d/%Y',        # 01/01/2023
        '%Y-%m-%d',        # 2023-01-01
        '%m-%d-%Y',        # 01-01-2023
        '%d %B %Y',        # 1 January 2023
        '%d %b %Y',        # 1 Jan 2023
        '%B %d-%d, %Y',    # January 1-2, 2023
        '%b %d-%d, %Y',    # Jan 1-2, 2023
    ]
    
    for date_format in date_formats:
        try:
            parsed_date = datetime.strptime(date_string, date_format)
            return parsed_date.strftime('%Y-%m-%d')  # Return in ISO format
        except ValueError:
            continue
    
    # Handle date ranges by taking the start date
    date_range_match = re.search(r'(\w+ \d+)(?:st|nd|rd|th)?\s*[-–]\s*(?:\w+ )?(\d+)(?:st|nd|rd|th)?,?\s*(\d{4})', date_string)
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
    
    # Try to extract just month, day, year components
    date_pattern = r'(?:(?P<month>\w+)\.?\s+)?(?P<day>\d{1,2})(?:st|nd|rd|th)?,?\s*(?P<year>\d{4})'
    match = re.search(date_pattern, date_string)
    if match:
        components = match.groupdict()
        if components['month']:
            try:
                if len(components['month']) <= 3:
                    # Abbreviated month name
                    date_str = f"{components['month']} {components['day']}, {components['year']}"
                    parsed_date = datetime.strptime(date_str, '%b %d, %Y')
                else:
                    # Full month name
                    date_str = f"{components['month']} {components['day']}, {components['year']}"
                    parsed_date = datetime.strptime(date_str, '%B %d, %Y')
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                pass
    
    # Handle relative dates (next week, next month, etc.)
    relative_date_map = {
        'today': datetime.now(),
        'tomorrow': datetime.now() + timedelta(days=1),
        'yesterday': datetime.now() - timedelta(days=1),
        'next week': datetime.now() + timedelta(days=7),
        'next month': datetime.now() + timedelta(days=30),
    }
    
    for key, value in relative_date_map.items():
        if key in date_string.lower():
            return value.strftime('%Y-%m-%d')
    
    # If no standard format matches, return original
    logger.warning(f"Could not parse date: {date_string}")
    return date_string

# Function to parse date range
def parse_date_range(date_string):
    """Parse a date range into start and end dates"""
    if not date_string or date_string == 'TBD':
        return {'start_date': None, 'end_date': None, 'days': None}
    
    # Look for date range patterns
    range_patterns = [
        # January 1-3, 2023
        r'(\w+)\s+(\d{1,2})(?:st|nd|rd|th)?\s*[-–]\s*(\d{1,2})(?:st|nd|rd|th)?,\s*(\d{4})',
        # Jan 1-3, 2023
        r'(\w{3})\s+(\d{1,2})(?:st|nd|rd|th)?\s*[-–]\s*(\d{1,2})(?:st|nd|rd|th)?,\s*(\d{4})',
        # January 1 - January 3, 2023
        r'(\w+)\s+(\d{1,2})(?:st|nd|rd|th)?\s*[-–]\s*(\w+)\s+(\d{1,2})(?:st|nd|rd|th)?,\s*(\d{4})',
        # Jan 1 - Jan 3, 2023
        r'(\w{3})\s+(\d{1,2})(?:st|nd|rd|th)?\s*[-–]\s*(\w{3})\s+(\d{1,2})(?:st|nd|rd|th)?,\s*(\d{4})',
        # January 1, 2023 - January 3, 2023
        r'(\w+)\s+(\d{1,2})(?:st|nd|rd|th)?,\s*(\d{4})\s*[-–]\s*(\w+)\s+(\d{1,2})(?:st|nd|rd|th)?,\s*(\d{4})',
        # Jan 1, 2023 - Jan 3, 2023
        r'(\w{3})\s+(\d{1,2})(?:st|nd|rd|th)?,\s*(\d{4})\s*[-–]\s*(\w{3})\s+(\d{1,2})(?:st|nd|rd|th)?,\s*(\d{4})'
    ]
    
    for pattern in range_patterns:
        match = re.search(pattern, date_string)
        if match:
            groups = match.groups()
            
            # Different handling based on number of groups
            if len(groups) == 4:  # Same month, different days
                month, start_day, end_day, year = groups
                try:
                    start_date = datetime.strptime(f"{month} {start_day}, {year}", '%B %d, %Y')
                except ValueError:
                    try:
                        start_date = datetime.strptime(f"{month} {start_day}, {year}", '%b %d, %Y')
                    except ValueError:
                        continue
                
                try:
                    end_date = datetime.strptime(f"{month} {end_day}, {year}", '%B %d, %Y')
                except ValueError:
                    try:
                        end_date = datetime.strptime(f"{month} {end_day}, {year}", '%b %d, %Y')
                    except ValueError:
                        continue
            
            elif len(groups) == 5:  # Different months
                if groups[0].isalpha() and groups[2].isalpha():  # Month names in positions 0 and 2
                    start_month, start_day, end_month, end_day, year = groups
                else:  # Full date range with different years
                    start_month, start_day, start_year, end_month, end_day = groups
                    year = start_year  # For initializing end_date
                
                try:
                    start_date = datetime.strptime(f"{start_month} {start_day}, {year}", '%B %d, %Y')
                except ValueError:
                    try:
                        start_date = datetime.strptime(f"{start_month} {start_day}, {year}", '%b %d, %Y')
                    except ValueError:
                        continue
                
                try:
                    end_date = datetime.strptime(f"{end_month} {end_day}, {year}", '%B %d, %Y')
                except ValueError:
                    try:
                        end_date = datetime.strptime(f"{end_month} {end_day}, {year}", '%b %d, %Y')
                    except ValueError:
                        continue
            
            elif len(groups) == 6:  # Different years
                start_month, start_day, start_year, end_month, end_day, end_year = groups
                
                try:
                    start_date = datetime.strptime(f"{start_month} {start_day}, {start_year}", '%B %d, %Y')
                except ValueError:
                    try:
                        start_date = datetime.strptime(f"{start_month} {start_day}, {start_year}", '%b %d, %Y')
                    except ValueError:
                        continue
                
                try:
                    end_date = datetime.strptime(f"{end_month} {end_day}, {end_year}", '%B %d, %Y')
                except ValueError:
                    try:
                        end_date = datetime.strptime(f"{end_month} {end_day}, {end_year}", '%b %d, %Y')
                    except ValueError:
                        continue
            
            else:
                continue
            
            # Calculate days
            days = (end_date - start_date).days + 1
            
            return {
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'days': days
            }
    
    # If no range found, treat as single day
    single_date = parse_date(date_string)
    if single_date and single_date != date_string:  # Successfully parsed
        return {
            'start_date': single_date,
            'end_date': single_date,
            'days': 1
        }
    
    return {
        'start_date': date_string,
        'end_date': date_string,
        'days': None
    }

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
    
    # Look for international locations
    international_match = re.search(r'([A-Za-z\s\.]+),\s*([A-Za-z\s]+)', text)
    if international_match:
        city, country = international_match.groups()
        return f"{city.strip()}, {country.strip()}"
    
    return text.strip()

# Function to extract golf course name
def extract_golf_course(text):
    """Extract golf course name from text"""
    if not text:
        return None
    
    # Common patterns for golf course names
    patterns = [
        r'at\s+([\w\s]+(?:Golf Club|Golf Course|Country Club|Golf & Country Club|Links))',
        r'([\w\s]+(?:Golf Club|Golf Course|Country Club|Golf & Country Club|Links))',
        r'course:\s*([\w\s]+)',
        r'venue:\s*([\w\s]+)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return match.group(1).strip()
    
    return None

# Function to generate a unique ID for a tournament
def generate_tournament_id(tournament_data):
    """Generate a unique ID for a tournament based on its data"""
    # Use name and date as key components
    key_data = f"{tournament_data.get('Tournament Name', '')}-{tournament_data.get('Date', '')}"
    
    # Add location if available
    if tournament_data.get('Location'):
        key_data += f"-{tournament_data.get('Location')}"
    
    # Generate hash
    return hashlib.md5(key_data.encode()).hexdigest()[:10]

# Function to get HTML content using requests
def get_page_html(url, timeout=15, max_retries=3):
    """Get HTML content from URL using requests with retries"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive'
    }
    
    retry_count = 0
    while retry_count < max_retries:
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            retry_count += 1
            if retry_count >= max_retries:
                st.error(f"Failed to fetch data from {url} after {max_retries} attempts. Error: {str(e)}")
                return None
            logger.warning(f"Attempt {retry_count} failed: {str(e)}. Retrying...")
            time.sleep(1)  # Wait before retrying

# Function to cache scraped data
def save_to_cache(key, data):
    """Save data to cache"""
    # Create cache directory if it doesn't exist
    cache_dir = ".cache"
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    
    # Save data to cache file
    cache_file = os.path.join(cache_dir, f"{key}.json")
    with open(cache_file, 'w') as f:
        json.dump(data, f)

# Function to load data from cache
def load_from_cache(key, max_age_hours=24):
    """Load data from cache if not expired"""
    cache_dir = ".cache"
    cache_file = os.path.join(cache_dir, f"{key}.json")
    
    # Check if cache file exists
    if not os.path.exists(cache_file):
        return None
    
    # Check if cache file is expired
    file_age = time.time() - os.path.getmtime(cache_file)
    if file_age > (max_age_hours * 3600):
        return None
    
    # Load data from cache file
    try:
        with open(cache_file, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None

# Function to determine if a tournament is a qualifier based on name
def is_qualifier_tournament(name):
    """Determine if a tournament is a qualifier based on name"""
    if not name:
        return False
    
    name_lower = name.lower()
    qualifier_keywords = [
        'qualifier', 'qualifying', 'qualification', 'q-school', 'q school',
        'local qualifying', 'sectional qualifying', 'regional qualifying'
    ]
    
    return any(keyword in name_lower for keyword in qualifier_keywords)

# Function to extract eligibility information
def extract_eligibility_info(html):
    """Extract eligibility requirements from tournament pages"""
    soup = BeautifulSoup(html, 'html.parser')
    eligibility_info = None
    
    # Look for eligibility sections
    eligibility_headers = soup.find_all(
        ['h1', 'h2', 'h3', 'h4', 'div', 'section'], 
        string=lambda s: s and re.search(r'eligib|who can play|entry requirements', s, re.I) if s else False
    )
    
    for header in eligibility_headers:
        # Look at section content
        eligibility_section = header.find_next(['div', 'ul', 'ol', 'p', 'section']) or header.parent
        
        if eligibility_section:
            eligibility_text = eligibility_section.get_text().strip()
            if eligibility_text and len(eligibility_text) > 10:
                eligibility_info = eligibility_text
                break
    
    # If no explicit eligibility section, look for eligibility keywords
    if not eligibility_info:
        eligibility_patterns = [
            r'(?:eligib[a-z]+)[^\.\n]*[:;-][^\.\n]+',
            r'open to[^\.\n]+',
            r'limited to[^\.\n]+',
            r'restricted to[^\.\n]+',
            r'who can play[^\.\n]*[:;-][^\.\n]+'
        ]
        
        for pattern in eligibility_patterns:
            eligibility_match = re.search(pattern, soup.get_text(), re.I)
            if eligibility_match:
                eligibility_info = eligibility_match.group(0).strip()
                break
    
    return eligibility_info

# Key function to extract qualifier information
def extract_qualifier_info(html, url, parent_tournament=None):
    """
    Extract qualifying round information from tournament pages
    
    Args:
        html: HTML content of the page
        url: URL of the page
        parent_tournament: Parent tournament data if available
        
    Returns:
        List of qualifiers with all available details
    """
    soup = BeautifulSoup(html, 'html.parser')
    qualifiers = []
    
    # 1. Look for qualifying sections/headers
    qualifier_headers = soup.find_all(
        ['h1', 'h2', 'h3', 'h4', 'div', 'section'], 
        string=lambda s: s and re.search(r'qualif|qualify|qualifying|qualifiers', s, re.I) if s else False
    )
    
    for header in qualifier_headers:
        # Look at section content
        qualifier_section = header.find_next(['div', 'ul', 'table', 'section', 'p']) or header.parent
        
        if qualifier_section:
            # Try to find qualifier items (list items, table rows, etc.)
            qualifier_items = qualifier_section.find_all(['li', 'tr', 'div'], class_=lambda c: c and ('item' in c.lower() or 'qualifier' in c.lower()) if c else False)
            
            # If specific items not found, try to extract from the entire section
            if not qualifier_items:
                qualifier_items = [qualifier_section]
            
            # Process each qualifier item/section
            for item in qualifier_items:
                item_text = item.get_text()
                
                # Extract date
                date_text = None
                date_match = re.search(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:st|nd|rd|th)?,?\s*\d{4}|\d{1,2}/\d{1,2}/\d{4}', item_text)
                if date_match:
                    date_text = date_match.group(0)
                
                # Extract location
                location_text = None
                location_match = re.search(r'([A-Za-z\s\.]+),\s*([A-Z]{2}|[A-Za-z\s]+)', item_text)
                if location_match:
                    city, state = location_match.groups()
                    location_text = f"{city.strip()}, {state.strip()}"
                
                # Extract golf course
                course_text = None
                course_match = re.search(r'(?:at|venue|location|course)[:;-]?\s*([\w\s\.\&\-\']+(?:Golf Club|Country Club|Golf Course|Golf & Country Club|Links|Course|G\.?C\.?))', item_text, re.I)
                if course_match:
                    course_text = course_match.group(1).strip()
                elif location_text:
                    # Try to extract course from link text or other elements
                    course_elements = item.find_all('a')
                    for element in course_elements:
                        element_text = element.get_text().strip()
                        if any(keyword in element_text.lower() for keyword in ['golf', 'country', 'club', 'course', 'links']):
                            course_text = element_text
                            break
                
                # If we found meaningful data, create a qualifier entry
                if date_text or location_text or course_text:
                    qualifier_data = {
                        'Tournament Name': f"{parent_tournament['Tournament Name']} Qualifier" if parent_tournament else "Qualifying Round",
                        'Date': parse_date(date_text) if date_text else None,
                        'Golf Course Name': course_text,
                        'Location': location_text,
                        'Tournament Type': 'Qualifying Round',
                        'Is Qualifier': True,
                        'Detail URL': url
                    }
                    
                    # Add parent tournament info if available
                    if parent_tournament:
                        qualifier_data['Parent Tournament'] = parent_tournament['Tournament Name']
                        qualifier_data['Parent Tournament ID'] = parent_tournament.get('Tournament ID')
                    
                    qualifiers.append(qualifier_data)
    
    # 2. Look for qualifying links
    qualifier_links = soup.find_all('a', 
        href=lambda href: href and re.search(r'qualif|qualifying|qualifier', href, re.I),
        string=lambda s: s and len(s) > 5
    )
    
    # Also look for links that contain text about qualifying
    qualifier_text_links = soup.find_all('a', 
        string=lambda s: s and re.search(r'qualif|qualifying|qualifier', s, re.I) if s else False
    )
    
    # Combine the link lists and remove duplicates
    all_qualifier_links = list(set(qualifier_links + qualifier_text_links))
    
    # Process each qualifier link
    for link in all_qualifier_links:
        link_text = link.get_text().strip()
        link_url = link.get('href', '')
        
        # Make absolute URL if needed
        if link_url and not link_url.startswith(('http://', 'https://')):
            link_url = urljoin(url, link_url)
        
        # Extract date from link text
        date_text = None
        date_match = re.search(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:st|nd|rd|th)?,?\s*\d{4}|\d{1,2}/\d{1,2}/\d{4}', link_text)
        if date_match:
            date_text = date_match.group(0)
        
        # Extract location from link text
        location_text = None
        location_match = re.search(r'([A-Za-z\s\.]+),\s*([A-Z]{2}|[A-Za-z\s]+)', link_text)
        if location_match:
            city, state = location_match.groups()
            location_text = f"{city.strip()}, {state.strip()}"
        
        # Extract golf course from link text
        course_text = None
        course_match = re.search(r'(?:at|venue|location|course)[:;-]?\s*([\w\s\.\&\-\']+(?:Golf Club|Country Club|Golf Course|Golf & Country Club|Links|Course|G\.?C\.?))', link_text, re.I)
        if course_match:
            course_text = course_match.group(1).strip()
        
        # Create qualifier entry
        qualifier_data = {
            'Tournament Name': f"{parent_tournament['Tournament Name']} Qualifier" if parent_tournament else "Qualifying Round",
            'Date': parse_date(date_text) if date_text else None,
            'Golf Course Name': course_text,
            'Location': location_text,
            'Tournament Type': 'Qualifying Round',
            'Is Qualifier': True,
            'Detail URL': link_url if link_url else url
        }
        
        # Add parent tournament info if available
        if parent_tournament:
            qualifier_data['Parent Tournament'] = parent_tournament['Tournament Name']
            qualifier_data['Parent Tournament ID'] = parent_tournament.get('Tournament ID')
        
        # Only add if we have meaningful data
        if link_url or date_text or location_text or course_text:
            qualifiers.append(qualifier_data)
    
    # 3. Special case: If this page IS a qualifying round page
    if (re.search(r'qualif|qualifying|qualifier', url, re.I) or 
        (soup.title and soup.title.string and re.search(r'qualif|qualifying|qualifier', soup.title.string, re.I))):
        
        # Extract main tournament data as a qualifier
        main_data = {
            'Tournament Name': None,
            'Date': None,
            'Golf Course Name': None,
            'Location': None,
            'Tournament Type': 'Qualifying Round',
            'Is Qualifier': True,
            'Detail URL': url
        }
        
        # Try to extract name
        title_element = soup.find('title') or soup.find('h1')
        if title_element:
            main_data['Tournament Name'] = title_element.get_text().strip()
        
        # Extract date
        date_element = soup.find(['span', 'div', 'p'], class_=lambda c: c and 'date' in c.lower() if c else False)
        if date_element:
            date_text = date_element.get_text().strip()
            main_data['Date'] = parse_date(date_text)
        else:
            # Try to find date pattern in the page
            date_match = re.search(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:st|nd|rd|th)?,?\s*\d{4}|\d{1,2}/\d{1,2}/\d{4}', soup.get_text())
            if date_match:
                main_data['Date'] = parse_date(date_match.group(0))
        
        # Extract location
        location_element = soup.find(['span', 'div', 'p'], class_=lambda c: c and 'location' in c.lower() if c else False)
        if location_element:
            location_text = location_element.get_text().strip()
            main_data['Location'] = extract_location(location_text)
        else:
            # Try to find location pattern in the page
            location_match = re.search(r'([A-Za-z\s\.]+),\s*([A-Z]{2}|[A-Za-z\s]+)', soup.get_text())
            if location_match:
                    city, state = location_match.groups()
                    main_data['Location'] = f"{city.strip()}, {state.strip()}"
        
        # Extract golf course
        course_element = soup.find(['span', 'div', 'p'], class_=lambda c: c and ('course' in c.lower() or 'venue' in c.lower()) if c else False)
        if course_element:
            course_text = course_element.get_text().strip()
            main_data['Golf Course Name'] = course_text
        else:
            # Try to find course pattern in the page
            course_match = re.search(r'(?:at|venue|location|course)[:;-]?\s*([\w\s\.\&\-\']+(?:Golf Club|Country Club|Golf Course|Golf & Country Club|Links|Course|G\.?C\.?))', soup.get_text(), re.I)
            if course_match:
                main_data['Golf Course Name'] = course_match.group(1).strip()
        
        # Add parent tournament info if available
        if parent_tournament:
            main_data['Parent Tournament'] = parent_tournament['Tournament Name']
            main_data['Parent Tournament ID'] = parent_tournament.get('Tournament ID')
        
        # Only add if we have a name
        if main_data['Tournament Name'] and len(main_data['Tournament Name']) > 5:
            qualifiers.append(main_data)
    
    return qualifiers

# Function to handle site type detection
def detect_site_type(url, html=None):
    """Detect the type of golf site from URL and HTML content"""
    # Check for common golf site patterns in URL
    if 'fsga.org' in url.lower():
        return 'fsga'
    elif 'golfgenius' in url.lower():
        return 'golfgenius'
    elif 'bluegolf' in url.lower():
        return 'bluegolf'
    
    # If HTML is provided, check content for site indicators
    if html:
        soup = BeautifulSoup(html, 'html.parser')
        
        # Check for FSGA
        if re.search(r'Florida State Golf Association|FSGA', soup.get_text()):
            return 'fsga'
        
        # Check for GolfGenius
        if re.search(r'GolfGenius|Golf Genius', soup.get_text()):
            return 'golfgenius'
        
        # Check for BlueGolf
        if re.search(r'BlueGolf|Blue Golf', soup.get_text()) or 'bluegolf.com' in str(soup):
            return 'bluegolf'
    
    # Default to generic
    return 'generic'

# Generic parser for tournament items
def parse_generic_tournament_item(element, base_url):
    """Parse a generic tournament list item"""
    # Initialize data
    tournament_data = {
        'Tournament Name': None,
        'Date': None,
        'Golf Course Name': None,
        'Location': None,
        'Tournament Type': None,
        'Is Qualifier': False,
        'Has Qualifiers': None,
        'Detail URL': None,
        'Eligibility': None
    }
    
    # Try to find the tournament name
    name_element = None
    name_selectors = [
        'a', 'h3', 'h4', 'span.title', 'div.title', 
        '.name', '.tournament-name', '.event-name',
        'td:first-child'
    ]
    
    for selector in name_selectors:
        name_element = element.select_one(selector)
        if name_element:
            break
    
    # If still no name element, try to get text directly
    if not name_element:
        # Check if element itself is a name container
        if element.name in ['a', 'h3', 'h4']:
            name_element = element
        else:
            # Otherwise, use element text if it's reasonably short
            text = element.get_text().strip()
            if text and len(text) < 100:
                tournament_data['Tournament Name'] = text
    
    # Extract name if element found
    if name_element and not tournament_data['Tournament Name']:
        tournament_data['Tournament Name'] = name_element.get_text().strip()
    
    # Skip if no name found or too short
    if not tournament_data['Tournament Name'] or len(tournament_data['Tournament Name']) < 5:
        return None
    
    # Extract detail URL
    if element.name == 'a' and 'href' in element.attrs:
        url = element['href']
        # Make absolute URL
        if not url.startswith(('http://', 'https://')):
            url = urljoin(base_url, url)
        tournament_data['Detail URL'] = url
    else:
        # Look for a link inside the element
        link = element.find('a')
        if link and 'href' in link.attrs:
            url = link['href']
            # Make absolute URL
            if not url.startswith(('http://', 'https://')):
                url = urljoin(base_url, url)
            tournament_data['Detail URL'] = url
    
    # Extract date
    date_element = None
    date_selectors = [
        '.date', 'span.date', 'div.date', 
        '.tournament-date', '.event-date',
        'td:nth-child(2)', 'td:nth-child(3)'
    ]
    
    for selector in date_selectors:
        date_element = element.select_one(selector)
        if date_element:
            break
    
    # If date element found, parse the date
    if date_element:
        date_text = date_element.get_text().strip()
        tournament_data['Date'] = parse_date(date_text)
    else:
        # Try to find date pattern in the element text
        element_text = element.get_text()
        date_match = re.search(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:st|nd|rd|th)?,?\s*\d{4}|\d{1,2}/\d{1,2}/\d{4}', element_text)
        if date_match:
            tournament_data['Date'] = parse_date(date_match.group(0))
    
    # Extract location
    location_element = None
    location_selectors = [
        '.location', 'span.location', 'div.location',
        '.tournament-location', '.event-location',
        'td:nth-child(4)', 'td:nth-child(3)'
    ]
    
    for selector in location_selectors:
        location_element = element.select_one(selector)
        if location_element:
            break
    
    # If location element found, extract location
    if location_element:
        location_text = location_element.get_text().strip()
        tournament_data['Location'] = extract_location(location_text)
    else:
        # Try to find location pattern in the element text
        element_text = element.get_text()
        location_match = re.search(r'([A-Za-z\s\.]+),\s*([A-Z]{2}|[A-Za-z\s]+)', element_text)
        if location_match:
            city, state = location_match.groups()
            tournament_data['Location'] = f"{city.strip()}, {state.strip()}"
    
    # Extract golf course
    course_element = None
    course_selectors = [
        '.course', 'span.course', 'div.course',
        '.venue', '.golf-course',
        'td:nth-child(5)'
    ]
    
    for selector in course_selectors:
        course_element = element.select_one(selector)
        if course_element:
            break
    
    # If course element found, extract course
    if course_element:
        course_text = course_element.get_text().strip()
        tournament_data['Golf Course Name'] = course_text
    else:
        # Try to find course pattern in the element text
        element_text = element.get_text()
        course_match = re.search(r'(?:at|venue|location|course)[:;-]?\s*([\w\s\.\&\-\']+(?:Golf Club|Country Club|Golf Course|Golf & Country Club|Links|Club|Course))', element_text, re.I)
        if course_match:
            tournament_data['Golf Course Name'] = course_match.group(1).strip()
    
    # Determine tournament type and qualifier status
    tournament_data['Is Qualifier'] = is_qualifier_tournament(tournament_data['Tournament Name'])
    
    if tournament_data['Is Qualifier']:
        tournament_data['Tournament Type'] = 'Qualifying Round'
    else:
        # Determine general tournament type
        name_lower = tournament_data['Tournament Name'].lower()
        
        if 'amateur' in name_lower:
            tournament_data['Tournament Type'] = 'Amateur'
        elif 'junior' in name_lower or 'youth' in name_lower:
            tournament_data['Tournament Type'] = 'Junior'
        elif 'senior' in name_lower:
            tournament_data['Tournament Type'] = 'Senior'
        elif 'open' in name_lower or 'championship' in name_lower:
            tournament_data['Tournament Type'] = 'Championship'
        else:
            tournament_data['Tournament Type'] = 'Tournament'
    
    # Generate unique ID
    tournament_data['Tournament ID'] = generate_tournament_id(tournament_data)
    
    return tournament_data

# Generic parser for tournament detail pages
def parse_generic_tournament_detail(soup, url):
    """Parse a generic tournament detail page"""
    # Initialize data
    tournament_data = {
        'Tournament Name': None,
        'Date': None,
        'Golf Course Name': None,
        'Location': None,
        'Tournament Type': None,
        'Is Qualifier': False,
        'Has Qualifiers': None,
        'Detail URL': url,
        'Eligibility': None,
        'Description': None
    }
    
    # Extract tournament name
    name_element = soup.select_one('h1, h2, .title, .tournament-title, .event-title')
    if name_element:
        tournament_data['Tournament Name'] = name_element.get_text().strip()
    else:
        # Try to extract from title
        title_element = soup.select_one('title')
        if title_element:
            title_text = title_element.get_text().strip()
            # Remove any website name suffix
            title_parts = title_text.split(' - ')
            if title_parts:
                tournament_data['Tournament Name'] = title_parts[0].strip()
    
    # Skip if no name found
    if not tournament_data['Tournament Name']:
        return None
    
    # Extract date
    date_element = soup.select_one('.date, .tournament-date, .event-date')
    if date_element:
        date_text = date_element.get_text().strip()
        tournament_data['Date'] = parse_date(date_text)
    else:
        # Try to find date pattern in the page
        date_match = re.search(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:st|nd|rd|th)?,?\s*\d{4}|\d{1,2}/\d{1,2}/\d{4}', soup.get_text())
        if date_match:
            tournament_data['Date'] = parse_date(date_match.group(0))
    
    # Extract location and golf course
    location_element = soup.select_one('.location, .tournament-location, .event-location')
    if location_element:
        location_text = location_element.get_text().strip()
        tournament_data['Location'] = extract_location(location_text)
    else:
        # Try to find location pattern in the page
        location_match = re.search(r'([A-Za-z\s\.]+),\s*([A-Z]{2}|[A-Za-z\s]+)', soup.get_text())
        if location_match:
            city, state = location_match.groups()
            tournament_data['Location'] = f"{city.strip()}, {state.strip()}"
    
    # Extract golf course
    course_element = soup.select_one('.course, .venue, .golf-course')
    if course_element:
        course_text = course_element.get_text().strip()
        tournament_data['Golf Course Name'] = course_text
    else:
        # Try to find course pattern in the page
        course_match = re.search(r'(?:at|venue|location|course)[:;-]?\s*([\w\s\.\&\-\']+(?:Golf Club|Country Club|Golf Course|Golf & Country Club|Links|Club|Course))', soup.get_text(), re.I)
        if course_match:
            tournament_data['Golf Course Name'] = course_match.group(1).strip()
    
    # Extract description
    description_element = soup.select_one('.description, .details, .tournament-info, .event-info, .about')
    if description_element:
        description_text = description_element.get_text().strip()
        if description_text:
            tournament_data['Description'] = description_text
    
    # Determine tournament type and qualifier status
    tournament_data['Is Qualifier'] = is_qualifier_tournament(tournament_data['Tournament Name'])
    
    if tournament_data['Is Qualifier']:
        tournament_data['Tournament Type'] = 'Qualifying Round'
    else:
        # Determine general tournament type
        name_lower = tournament_data['Tournament Name'].lower()
        
        if 'amateur' in name_lower:
            tournament_data['Tournament Type'] = 'Amateur'
        elif 'junior' in name_lower or 'youth' in name_lower:
            tournament_data['Tournament Type'] = 'Junior'
        elif 'senior' in name_lower:
            tournament_data['Tournament Type'] = 'Senior'
        elif 'open' in name_lower or 'championship' in name_lower:
            tournament_data['Tournament Type'] = 'Championship'
        else:
            tournament_data['Tournament Type'] = 'Tournament'
    
    # Generate unique ID
    tournament_data['Tournament ID'] = generate_tournament_id(tournament_data)
    
    return tournament_data

# Parse BlueGolf tournament item
def parse_bluegolf_tournament_item(element, base_url):
    """Parse a BlueGolf tournament list item"""
    # Initialize data
    tournament_data = {
        'Tournament Name': None,
        'Date': None,
        'Golf Course Name': None,
        'Location': None,
        'Tournament Type': None,
        'Is Qualifier': False,
        'Has Qualifiers': None,
        'Detail URL': None,
        'Eligibility': None
    }
    
    # Try to find the tournament name
    name_element = element.find('a') or element.find(['td', 'div'], class_=lambda c: c and ('name' in c.lower() or 'title' in c.lower()) if c else False)
    
    if name_element:
        tournament_data['Tournament Name'] = name_element.get_text().strip()
    else:
        # Try to extract from the element text
        text = element.get_text().strip()
        if text and len(text) < 100:
            # Try to extract name as first line or sentence
            name_match = re.match(r'^([^\n\.]+)', text)
            if name_match:
                tournament_data['Tournament Name'] = name_match.group(1).strip()
    
    # Skip if no name found or too short
    if not tournament_data['Tournament Name'] or len(tournament_data['Tournament Name']) < 5:
        return None
    
    # Extract detail URL
    link = element.find('a')
    if link and 'href' in link.attrs:
        url = link['href']
        # Make absolute URL
        if not url.startswith(('http://', 'https://')):
            url = urljoin(base_url, url)
        tournament_data['Detail URL'] = url
    
    # Extract date
    date_element = element.find(['td', 'div'], class_=lambda c: c and 'date' in c.lower() if c else False)
    if date_element:
        date_text = date_element.get_text().strip()
        tournament_data['Date'] = parse_date(date_text)
    else:
        # Try to find date pattern in the element text
        date_match = re.search(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:st|nd|rd|th)?,?\s*\d{4}|\d{1,2}/\d{1,2}/\d{4}', element.get_text())
        if date_match:
            tournament_data['Date'] = parse_date(date_match.group(0))
    
    # Extract location and golf course
    location_element = element.find(['td', 'div'], class_=lambda c: c and ('location' in c.lower() or 'course' in c.lower()) if c else False)
    if location_element:
        location_text = location_element.get_text().strip()
        
        # Try to extract location
        location = extract_location(location_text)
        if location:
            tournament_data['Location'] = location
        
        # Try to extract golf course
        course = extract_golf_course(location_text)
        if course:
            tournament_data['Golf Course Name'] = course
    else:
        # Try to find location and course patterns in the element text
        element_text = element.get_text()
        
        # Location pattern
        location_match = re.search(r'([A-Za-z\s\.]+),\s*([A-Z]{2}|[A-Za-z\s]+)', element_text)
        if location_match:
            city, state = location_match.groups()
            tournament_data['Location'] = f"{city.strip()}, {state.strip()}"
        
        # Course pattern
        course_match = re.search(r'(?:at|venue|location|course)[:;-]?\s*([\w\s\.\&\-\']+(?:Golf Club|Country Club|Golf Course|Golf & Country Club|Links|Club|Course))', element_text, re.I)
        if course_match:
            tournament_data['Golf Course Name'] = course_match.group(1).strip()
    
    # Determine tournament type and qualifier status
    tournament_data['Is Qualifier'] = is_qualifier_tournament(tournament_data['Tournament Name'])
    
    if tournament_data['Is Qualifier']:
        tournament_data['Tournament Type'] = 'Qualifying Round'
    else:
        # Determine general tournament type
        name_lower = tournament_data['Tournament Name'].lower()
        
        if 'amateur' in name_lower:
            tournament_data['Tournament Type'] = 'Amateur'
        elif 'junior' in name_lower or 'youth' in name_lower:
            tournament_data['Tournament Type'] = 'Junior'
        elif 'senior' in name_lower:
            tournament_data['Tournament Type'] = 'Senior'
        elif 'open' in name_lower or 'championship' in name_lower:
            tournament_data['Tournament Type'] = 'Championship'
        else:
            tournament_data['Tournament Type'] = 'Tournament'
    
    # Generate unique ID
    tournament_data['Tournament ID'] = generate_tournament_id(tournament_data)
    
    return tournament_data

# FSGA site-specific parser for tournament items
def parse_fsga_tournament_item(element, base_url):
    """Parse an FSGA tournament list item"""
    # Initialize data
    tournament_data = {
        'Tournament Name': None,
        'Date': None,
        'Golf Course Name': None,
        'Location': None,
        'Tournament Type': None,
        'Is Qualifier': False,
        'Has Qualifiers': None,
        'Detail URL': None,
        'Eligibility': None
    }
    
    # Try to find the tournament name
    name_element = None
    name_selectors = [
        'a.event-title', 'a.tournament-title', 'a.tournament-name', 
        'span.event-title', 'span.tournament-title', 'div.event-title',
        'h3', 'h4', 'a'
    ]
    
    for selector in name_selectors:
        name_element = element.select_one(selector)
        if name_element:
            break
    
    # If still no name element, try to get text directly
    if not name_element:
        # If element is an 'a' tag, use it directly
        if element.name == 'a':
            name_element = element
        else:
            # Otherwise, check if there's text content
            text = element.get_text().strip()
            if text and len(text) > 5:
                tournament_data['Tournament Name'] = text
    
    # Extract name if element found
    if name_element and not tournament_data['Tournament Name']:
        tournament_data['Tournament Name'] = name_element.get_text().strip()
    
    # Skip if no name found or too short
    if not tournament_data['Tournament Name'] or len(tournament_data['Tournament Name']) < 5:
        return None
    
    # Extract detail URL
    link_element = element.find('a', href=lambda href: href and ('/Tournament/Details/' in href or '/Tournament/Index/' in href))
    if link_element and 'href' in link_element.attrs:
        url = link_element['href']
        # Make absolute URL
        if not url.startswith(('http://', 'https://')):
            url = urljoin(base_url, url)
        tournament_data['Detail URL'] = url
    elif name_element and name_element.name == 'a' and 'href' in name_element.attrs:
        url = name_element['href']
        # Make absolute URL
        if not url.startswith(('http://', 'https://')):
            url = urljoin(base_url, url)
        tournament_data['Detail URL'] = url
    
    # Extract date using various selectors
    date_element = None
    date_selectors = [
        'span.date', 'div.date', 'span.tournament-date', 'div.tournament-date',
        'span.event-date', 'div.event-date', 'td:nth-child(2)'
    ]
    
    for selector in date_selectors:
        date_element = element.select_one(selector)
        if date_element:
            break
    
    # If date element found, parse the date
    if date_element:
        date_text = date_element.get_text().strip()
        tournament_data['Date'] = parse_date(date_text)
        
        # Parse date range
        date_range = parse_date_range(date_text)
        if date_range.get('days'):
            tournament_data['Start Date'] = date_range.get('start_date')
            tournament_data['End Date'] = date_range.get('end_date')
            tournament_data['Days'] = date_range.get('days')
    else:
        # Try to find date pattern in the element text
        element_text = element.get_text()
        date_match = re.search(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:st|nd|rd|th)?,?\s*\d{4}|\d{1,2}/\d{1,2}/\d{4}', element_text)
        if date_match:
            tournament_data['Date'] = parse_date(date_match.group(0))
    
    # Extract location using various selectors
    location_element = None
    location_selectors = [
        'span.location', 'div.location', 'span.tournament-location', 
        'div.tournament-location', 'span.event-location', 'div.event-location',
        'td:nth-child(3)'
    ]
    
    for selector in location_selectors:
        location_element = element.select_one(selector)
        if location_element:
            break
    
    # If location element found, extract location
    if location_element:
        location_text = location_element.get_text().strip()
        tournament_data['Location'] = extract_location(location_text)
        
        # Try to extract golf course name from location
        course = extract_golf_course(location_text)
        if course:
            tournament_data['Golf Course Name'] = course
    else:
        # Try to find location pattern in the element text
        element_text = element.get_text()
        location_match = re.search(r'([A-Za-z\s\.]+),\s*([A-Z]{2}|[A-Za-z\s]+)', element_text)
        if location_match:
            city, state = location_match.groups()
            tournament_data['Location'] = f"{city.strip()}, {state.strip()}"
    
    # Determine tournament type and qualifier status
    tournament_data['Is Qualifier'] = is_qualifier_tournament(tournament_data['Tournament Name'])
    
    if tournament_data['Is Qualifier']:
        tournament_data['Tournament Type'] = 'Qualifying Round'
    else:
        tournament_data['Tournament Type'] = determine_tournament_type(tournament_data['Tournament Name'])
    
    # Generate unique ID
    tournament_data['Tournament ID'] = generate_tournament_id(tournament_data)
    
    return tournament_data

# FSGA site-specific parser for tournament detail
def parse_fsga_tournament_detail(soup, url):
    """Parse FSGA tournament detail page"""
    # Initialize data
    tournament_data = {
        'Tournament Name': None,
        'Date': None,
        'Golf Course Name': None,
        'Location': None,
        'Tournament Type': None,
        'Is Qualifier': False,
        'Has Qualifiers': None,
        'Detail URL': url,
        'Eligibility': None,
        'Description': None
    }
    
    # Extract tournament name
    name_element = soup.select_one('h1.tournament-title, h2.tournament-title, h1, h2')
    if name_element:
        tournament_data['Tournament Name'] = name_element.get_text().strip()
    else:
        # Try to extract from title
        title_element = soup.select_one('title')
        if title_element:
            title_text = title_element.get_text().strip()
            # Remove any website name suffix
            title_parts = title_text.split(' - ')
            if title_parts:
                tournament_data['Tournament Name'] = title_parts[0].strip()
    
    # Skip if no name found
    if not tournament_data['Tournament Name']:
        return None
    
    # Extract date
    date_element = soup.select_one('.tournament-date, .event-date, .date-display')
    if date_element:
        date_text = date_element.get_text().strip()
        tournament_data['Date'] = parse_date(date_text)
        
        # Parse date range
        date_range = parse_date_range(date_text)
        if date_range.get('days'):
            tournament_data['Start Date'] = date_range.get('start_date')
            tournament_data['End Date'] = date_range.get('end_date')
            tournament_data['Days'] = date_range.get('days')
    else:
        # Try to find date pattern in the page
        date_match = re.search(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:st|nd|rd|th)?,?\s*\d{4}|\d{1,2}/\d{1,2}/\d{4}', soup.get_text())
        if date_match:
            tournament_data['Date'] = parse_date(date_match.group(0))
    
    # Extract location and golf course
    location_element = soup.select_one('.tournament-location, .event-location, .location-display')
    if location_element:
        location_text = location_element.get_text().strip()
        tournament_data['Location'] = extract_location(location_text)
        
        # Try to extract golf course name
        course = extract_golf_course(location_text)
        if course:
            tournament_data['Golf Course Name'] = course
    else:
        # Try alternative elements
        venue_element = soup.select_one('.venue, .course, .golf-course')
        if venue_element:
            venue_text = venue_element.get_text().strip()
            tournament_data['Golf Course Name'] = venue_text
            
            # Try to extract location from venue
            location = extract_location(venue_text)
            if location:
                tournament_data['Location'] = location
    
    # If still no golf course, try to find it elsewhere in the page
    if not tournament_data['Golf Course Name']:
        course_pattern = r'(?:at|venue|location|course)[:;-]?\s*([\w\s\.\&\-\']+(?:Golf Club|Country Club|Golf Course|Golf & Country Club))'
        match = re.search(course_pattern, soup.get_text(), re.I)
        if match:
            tournament_data['Golf Course Name'] = match.group(1).strip()
    
    # Extract description
    description_element = soup.select_one('.tournament-description, .event-description, .description, .details')
    if description_element:
        description_text = description_element.get_text().strip()
        if description_text:
            tournament_data['Description'] = description_text
    else:
        # Try to extract description from format information
        format_element = soup.select_one('#Format, .format, #format')
        if format_element:
            format_text = format_element.get_text().strip()
            if format_text:
                tournament_data['Description'] = format_text
    
    # Extract eligibility
    eligibility_element = soup.select_one('#Eligibility, .eligibility, #eligibility')
    if eligibility_element:
        eligibility_text = eligibility_element.get_text().strip()
        if eligibility_text:
            tournament_data['Eligibility'] = eligibility_text
    else:
        # Try to find eligibility information in the text
        eligibility_info = extract_eligibility_info(soup.prettify())
        if eligibility_info:
            tournament_data['Eligibility'] = eligibility_info
    
    # Determine tournament type and qualifier status
    tournament_data['Is Qualifier'] = is_qualifier_tournament(tournament_data['Tournament Name'])
    
    if tournament_data['Is Qualifier']:
        tournament_data['Tournament Type'] = 'Qualifying Round'
    else:
        tournament_data['Tournament Type'] = determine_tournament_type(
            tournament_data['Tournament Name'], 
            tournament_data.get('Description', '')
        )
    
    # Generate unique ID
    tournament_data['Tournament ID'] = generate_tournament_id(tournament_data)
    
    return tournament_data

# Function to extract qualifying information from FSGA tournament
def parse_fsga_qualifiers(soup, url, parent_tournament):
    """Extract qualifier information from an FSGA tournament page"""
    qualifiers = []
    
    # Look for qualifier sections or links
    qualifier_headers = soup.find_all(['h3', 'h4', 'div', 'a'], string=lambda s: s and re.search(r'qualify|qualifying|qualifiers', s, re.I) if s else False)
    
    for header in qualifier_headers:
        # Check if this is a link to qualifiers
        if header.name == 'a' and 'href' in header.attrs:
            link_url = header['href']
            # Make absolute URL
            if not link_url.startswith(('http://', 'https://')):
                link_url = urljoin(url, link_url)
            
            # Create a qualifier entry with the information we have
            qualifier_data = {
                'Tournament Name': f"{parent_tournament['Tournament Name']} - Qualifier",
                'Date': None,
                'Golf Course Name': None,
                'Location': None,
                'Tournament Type': 'Qualifying Round',
                'Is Qualifier': True,
                'Detail URL': link_url,
                'Parent Tournament': parent_tournament['Tournament Name'],
                'Parent Tournament ID': parent_tournament.get('Tournament ID')
            }
            
            # Try to extract date and location from link text
            link_text = header.get_text()
            
            # Date
            date_match = re.search(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:st|nd|rd|th)?,?\s*\d{4}|\d{1,2}/\d{1,2}/\d{4}', link_text)
            if date_match:
                qualifier_data['Date'] = parse_date(date_match.group(0))
            
            # Location
            location_match = re.search(r'([A-Za-z\s\.]+),\s*([A-Z]{2}|[A-Za-z\s]+)', link_text)
            if location_match:
                city, state = location_match.groups()
                qualifier_data['Location'] = f"{city.strip()}, {state.strip()}"
            
            qualifiers.append(qualifier_data)
        else:
            # Look at next section for qualifier details
            qualifier_section = header.find_next(['div', 'ul', 'table', 'ol'])
            
            if qualifier_section:
                # Try to find qualifier items
                qualifier_items = qualifier_section.find_all(['li', 'tr'])
                
                if not qualifier_items:
                    # If no items found, extract information from the section text
                    section_text = qualifier_section.get_text()
                    
                    # Look for date patterns
                    date_matches = re.finditer(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:st|nd|rd|th)?,?\s*\d{4}|\d{1,2}/\d{1,2}/\d{4}', section_text)
                    
                    for date_match in date_matches:
                        date_text = date_match.group(0)
                        
                        # Get surrounding context (100 chars before and after the date)
                        start_pos = max(0, date_match.start() - 100)
                        end_pos = min(len(section_text), date_match.end() + 100)
                        context = section_text[start_pos:end_pos]
                        
                        # Look for location in context
                        location_match = re.search(r'([A-Za-z\s\.]+),\s*([A-Z]{2}|[A-Za-z\s]+)', context)
                        location_text = None
                        if location_match:
                            city, state = location_match.groups()
                            location_text = f"{city.strip()}, {state.strip()}"
                        
                        # Look for golf course in context
                        course_match = re.search(r'(?:at|venue|location|course)[:;-]?\s*([\w\s\.\&\-\']+(?:Golf Club|Country Club|Golf Course|Golf & Country Club))', context, re.I)
                        course_text = None
                        if course_match:
                            course_text = course_match.group(1).strip()
                        
                        # Create qualifier entry
                        qualifier_data = {
                            'Tournament Name': f"{parent_tournament['Tournament Name']} - Qualifier",
                            'Date': parse_date(date_text),
                            'Golf Course Name': course_text,
                            'Location': location_text,
                            'Tournament Type': 'Qualifying Round',
                            'Is Qualifier': True,
                            'Detail URL': url,  # Use parent tournament URL
                            'Parent Tournament': parent_tournament['Tournament Name'],
                            'Parent Tournament ID': parent_tournament.get('Tournament ID')
                        }
                        
                        qualifiers.append(qualifier_data)
                else:
                    # Process each qualifier item
                    for item in qualifier_items:
                        item_text = item.get_text()
                        
                        # Skip if this doesn't look like a qualifier item
                        if len(item_text) < 10:
                            continue
                        
                        # Extract date
                        date_match = re.search(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:st|nd|rd|th)?,?\s*\d{4}|\d{1,2}/\d{1,2}/\d{4}', item_text)
                        date_text = None
                        if date_match:
                            date_text = date_match.group(0)
                        
                        # Extract location
                        location_match = re.search(r'([A-Za-z\s\.]+),\s*([A-Z]{2}|[A-Za-z\s]+)', item_text)
                        location_text = None
                        if location_match:
                            city, state = location_match.groups()
                            location_text = f"{city.strip()}, {state.strip()}"
                        
                        # Extract golf course
                        course_match = re.search(r'(?:at|venue|location|course)[:;-]?\s*([\w\s\.\&\-\']+(?:Golf Club|Country Club|Golf Course|Golf & Country Club))', item_text, re.I)
                        course_text = None
                        if course_match:
                            course_text = course_match.group(1).strip()
                        
                        # Look for a link
                        link = item.find('a')
                        link_url = None
                        if link and 'href' in link.attrs:
                            link_url = link['href']
                            # Make absolute URL
                            if not link_url.startswith(('http://', 'https://')):
                                link_url = urljoin(url, link_url)
                        
                        # Create qualifier entry
                        qualifier_data = {
                            'Tournament Name': f"{parent_tournament['Tournament Name']} - Qualifier",
                            'Date': parse_date(date_text) if date_text else None,
                            'Golf Course Name': course_text,
                            'Location': location_text,
                            'Tournament Type': 'Qualifying Round',
                            'Is Qualifier': True,
                            'Detail URL': link_url if link_url else url,
                            'Parent Tournament': parent_tournament['Tournament Name'],
                            'Parent Tournament ID': parent_tournament.get('Tournament ID')
                        }
                        
                        qualifiers.append(qualifier_data)
    
    # Special case: look for FSGA qualifier links
    qualifier_links = soup.find_all('a', href=lambda href: href and ('/Tournament/Qualifier/' in href or '/Qualifier/' in href))
    
    for link in qualifier_links:
        link_text = link.get_text().strip()
        link_url = link['href']
        
        # Make absolute URL
        if not link_url.startswith(('http://', 'https://')):
            link_url = urljoin(url, link_url)
        
        # Create qualifier entry
        qualifier_data = {
            'Tournament Name': f"{parent_tournament['Tournament Name']} - Qualifier",
            'Date': None,
            'Golf Course Name': None,
            'Location': None,
            'Tournament Type': 'Qualifying Round',
            'Is Qualifier': True,
            'Detail URL': link_url,
            'Parent Tournament': parent_tournament['Tournament Name'],
            'Parent Tournament ID': parent_tournament.get('Tournament ID')
        }
        
        # Try to extract date and location from link text
        # Date
        date_match = re.search(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:st|nd|rd|th)?,?\s*\d{4}|\d{1,2}/\d{1,2}/\d{4}', link_text)
        if date_match:
            qualifier_data['Date'] = parse_date(date_match.group(0))
        
        # Location
        location_match = re.search(r'([A-Za-z\s\.]+),\s*([A-Z]{2}|[A-Za-z\s]+)', link_text)
        if location_match:
            city, state = location_match.groups()
            qualifier_data['Location'] = f"{city.strip()}, {state.strip()}"
        
        qualifiers.append(qualifier_data)
    
    # Look for qualifying sites text
    qualifier_sites = soup.find(string=lambda text: text and re.search(r'\d+\s+qualifying sites', text, re.I))
    if qualifier_sites:
        # If we find text about qualifier sites but no specific qualifiers,
        # mark the parent tournament as having qualifiers
        if not qualifiers:
            # Try to extract the number of qualifying sites
            sites_match = re.search(r'(\d+)\s+qualifying sites', qualifier_sites, re.I)
            if sites_match:
                parent_tournament['Qualifier Count'] = int(sites_match.group(1))
                parent_tournament['Has Qualifiers'] = True
    
    return qualifiers

# Function to scrape tournament data with focus on qualifiers and eligibility
def scrape_tournament_focused(url, max_details=None, show_progress=True):
    """Main function to scrape tournament data with focus on qualifiers and eligibility"""
    # Check cache first
    cache_key = f"focused_{hashlib.md5(url.encode()).hexdigest()}"
    cached_data = load_from_cache(cache_key)
    if cached_data:
        if show_progress:
            st.success(f"Loaded {len(cached_data)} tournaments from cache.")
        return cached_data
    
    # Get HTML content
    html = get_page_html(url)
    if not html:
        return []
    
    # Parse HTML
    soup = BeautifulSoup(html, 'html.parser')
    tournaments = []
    
    # Set up progress tracking
    if show_progress:
        progress_text = st.empty()
        progress_bar = st.progress(0.0)
        progress_text.text("Finding tournament elements...")
    
    # Detect site type
    site_type = detect_site_type(url, html)
    
    # Check if this is a tournament details page or a tournament list page
    is_detail_page = False
    
    # Look for detail page indicators
    if re.search(r'details|tournament/\d+|event/\d+', url.lower()):
        is_detail_page = True
    else:
        # Check content for detail page indicators
        detail_indicators = [
            'round information', 
            'tournament information',
            'player information',
            'championship format'
        ]
        
        for indicator in detail_indicators:
            if indicator in soup.get_text().lower():
                is_detail_page = True
                break
    
    if is_detail_page:
        # Parse single tournament detail page
        tournament = None
        
        if site_type == 'fsga':
            tournament = parse_fsga_tournament_detail(soup, url)
        elif site_type == 'golfgenius':
            tournament = parse_golfgenius_tournament_detail(soup, url)
        else:
            # Use generic parser
            tournament = parse_generic_tournament_detail(soup, url)
        
        if tournament:
            # Extract and add eligibility information
            eligibility_info = extract_eligibility_info(html)
            if eligibility_info:
                tournament['Eligibility'] = eligibility_info
            
            tournaments.append(tournament)
            
            # Extract qualifier information
            qualifiers = extract_qualifier_info(html, url, tournament)
            if qualifiers:
                tournaments.extend(qualifiers)
                
                # Update main tournament to indicate it has qualifiers
                tournament['Has Qualifiers'] = True
                tournament['Qualifier Count'] = len(qualifiers)
            else:
                tournament['Has Qualifiers'] = False
    else:
        # Parse tournament list page
        tournament_elements = []
        
        # Use site-specific selectors
        if site_type == 'fsga':
            selectors = [
                'table.tournament-results tbody tr',
                'div.tournament-item',
                '.events-list .event-item',
                '.tournament-list-item',
                'div.card',
                'div.event-card',
                'ul.event-list li'
            ]
        elif site_type == 'golfgenius':
            selectors = [
                '.event-row',
                '.tournament-item',
                '.event-card',
                '.event-list-item',
                '.event-box',
                '.tournament-box',
                '.tournament-card'
            ]
        elif site_type == 'bluegolf':
            selectors = [
                'table.tournamentItem',
                'tr.tournamentItem',
                'div.tournamentItem',
                '.eventRow',
                '.eventItem'
            ]
        else:
            # Generic selectors
            selectors = [
                'table tr',
                'div.item',
                'div.event',
                'div.tournament',
                'li',
                'article',
                'div.card'
            ]
        
        # Try each selector
        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                tournament_elements.extend(elements)
        
        # If still no elements found, look for any links that might be tournaments
        if not tournament_elements:
            links = soup.find_all('a', href=lambda href: href and (
                re.search(r'tournament|event|championship|open|amateur|qualifier', href.lower()) if href else False
            ))
            
            for link in links:
                # Try to find a parent element that might contain more tournament info
                parent = link.find_parent(['div', 'li', 'tr'])
                if parent:
                    tournament_elements.append(parent)
                else:
                    # If no suitable parent, just use the link itself
                    tournament_elements.append(link)
        
        # Get total count for progress calculation
        total_elements = len(tournament_elements)
        
        # Process each tournament element
        for i, element in enumerate(tournament_elements):
            # Update progress
            if show_progress and total_elements > 0:
                progress = i / total_elements
                progress_bar.progress(progress)
                progress_text.text(f"Processing tournament {i+1} of {total_elements}...")
            
            # Extract tournament info
            tournament = None
            
            if site_type == 'fsga':
                tournament = parse_fsga_tournament_item(element, url)
            elif site_type == 'golfgenius':
                tournament = parse_golfgenius_tournament_item(element, url)
            elif site_type == 'bluegolf':
                tournament = parse_bluegolf_tournament_item(element, url)
            else:
                tournament = parse_generic_tournament_item(element, url)
            
            if tournament:
                # Check if this is a qualifier based on name
                is_qualifier = is_qualifier_tournament(tournament['Tournament Name'])
                tournament['Is Qualifier'] = is_qualifier
                
                if is_qualifier:
                    tournament['Tournament Type'] = 'Qualifying Round'
                
                tournaments.append(tournament)
                
                # If we have a detail URL and within the max_details limit, get additional info
                if tournament.get('Detail URL') and (max_details is None or i < max_details):
                    detail_html = get_page_html(tournament['Detail URL'])
                    if detail_html:
                        # Extract and add eligibility information
                        eligibility_info = extract_eligibility_info(detail_html)
                        if eligibility_info:
                            tournament['Eligibility'] = eligibility_info
                        
                        # Extract qualifier information
                        qualifiers = extract_qualifier_info(detail_html, tournament['Detail URL'], tournament)
                        if qualifiers:
                            tournaments.extend(qualifiers)
                            
                            # Update main tournament to indicate it has qualifiers
                            tournament['Has Qualifiers'] = True
                            tournament['Qualifier Count'] = len(qualifiers)
                        else:
                            tournament['Has Qualifiers'] = False
    
    # Complete progress
    if show_progress:
        progress_bar.progress(1.0)
        qualifier_count = sum(1 for t in tournaments if t.get('Is Qualifier'))
        progress_text.text(f"Found {len(tournaments)} tournaments (including {qualifier_count} qualifying rounds)")
    
    # Save to cache
    save_to_cache(cache_key, tournaments)
    
    return tournaments

# Streamlit UI for the focused golf scraper
def main():
    st.set_page_config(page_title="Golf Tournament & Qualifier Scraper", layout="wide")
    
    st.title("Golf Tournament & Qualifier Scraper")
    
    st.markdown("""
    ## Extract tournament and qualifier information from golf association websites
    
    This tool helps you gather information about tournaments and their qualifying rounds from various golf websites.
    Simply enter the URL of a tournament page and click "Scrape Tournaments".
    
    The scraper will identify:
    1. Tournaments and their details
    2. Whether tournaments have qualifying rounds
    3. Details of all qualifying rounds
    4. Eligibility information if no qualifying rounds are available
    """)
    
    # Sidebar for configuration
    st.sidebar.title("Configuration")
    
    # Input for URL
    url = st.sidebar.text_input(
        "Tournament Page URL",
        value="https://www.fsga.org/Tournament/Details/6031cb23-72d9-45d8-8b14-1c15808a1ae0"
    )
    
    # Advanced options
    with st.sidebar.expander("Advanced Options"):
        max_details = st.number_input("Maximum detail pages to scrape (0 = None)", min_value=0, value=10, step=1)
        max_details = None if max_details == 0 else max_details
        
        show_debug = st.checkbox("Show debug information", value=False)
        
        if show_debug:
            logging.getLogger().setLevel(logging.DEBUG)
        else:
            logging.getLogger().setLevel(logging.INFO)
    
    # Create tabs for different functionality
    tab1, tab2, tab3 = st.tabs(["Scraper", "Export", "Help"])
    
    with tab1:
        # Button to start scraping
        if st.button("Scrape Tournaments", type="primary"):
            if url:
                with st.spinner('Scraping data...'):
                    try:
                        # Use the focused scraper
                        tournaments = scrape_tournament_focused(url, max_details=max_details, show_progress=True)
                        
                        if not tournaments:
                            st.error("No tournament data found. The website structure might not be supported.")
                        else:
                            # Store the data in session state
                            st.session_state.tournaments = tournaments
                            
                            # Display results
                            st.success(f"Found {len(tournaments)} tournaments and qualifying rounds")
                            
                            # Count qualifiers
                            qualifier_count = sum(1 for t in tournaments if t.get('Is Qualifier'))
                            tournaments_with_qualifiers = sum(1 for t in tournaments if t.get('Has Qualifiers'))
                            
                            if qualifier_count > 0:
                                st.info(f"Including {qualifier_count} qualifying rounds for {tournaments_with_qualifiers} tournaments")
                            
                    except Exception as e:
                        st.error(f"An error occurred during scraping: {str(e)}")
                        if show_debug:
                            st.exception(e)
            else:
                st.error("Please enter a valid URL")
        
        # Initialize session state if not exists
        if 'tournaments' not in st.session_state:
            st.session_state.tournaments = []
        
        # Only show results if we have tournaments
        if st.session_state.tournaments:
            # Display results in two tables: Tournaments and Qualifiers
            st.subheader("Tournament Data")
            
            # Filter for main tournaments
            main_tournaments = [t for t in st.session_state.tournaments if not t.get('Is Qualifier')]
            
            # Create a more readable display table for main tournaments
            if main_tournaments:
                display_data = []
                for t in main_tournaments:
                    display_row = {
                        'Name': t.get('Tournament Name', ''),
                        'Date': t.get('Date', 'N/A'),
                        'Course': t.get('Golf Course Name', 'N/A'),
                        'Location': t.get('Location', 'N/A'),
                        'Has Qualifiers': '✓' if t.get('Has Qualifiers') else '✗',
                        'Qualifier Count': t.get('Qualifier Count', 0) if t.get('Has Qualifiers') else 0
                    }
                    
                    display_data.append(display_row)
                
                # Show data table
                st.dataframe(display_data, use_container_width=True)
            else:
                st.info("No main tournaments found.")
            
            # Filter for qualifiers
            qualifiers = [t for t in st.session_state.tournaments if t.get('Is Qualifier')]
            
            if qualifiers:
                st.subheader("Qualifying Rounds")
                
                # Create a more readable display table for qualifiers
                display_data = []
                for q in qualifiers:
                    display_row = {
                        'Name': q.get('Tournament Name', ''),
                        'Date': q.get('Date', 'N/A'),
                        'Course': q.get('Golf Course Name', 'N/A'),
                        'Location': q.get('Location', 'N/A'),
                        'Parent Tournament': q.get('Parent Tournament', 'N/A')
                    }
                    
                    display_data.append(display_row)
                
                # Show data table
                st.dataframe(display_data, use_container_width=True)
            else:
                st.info("No qualifying rounds found.")
            
            # Show tournament details on click
            if main_tournaments:
                st.subheader("Tournament Details")
                
                selected_tournament_name = st.selectbox(
                    "Select a tournament to view details", 
                    options=[t.get('Tournament Name', 'Unknown') for t in main_tournaments]
                )
                
                # Find the selected tournament
                selected_tournament = next(
                    (t for t in main_tournaments if t.get('Tournament Name') == selected_tournament_name), 
                    None
                )
                
                if selected_tournament:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Date:**", selected_tournament.get('Date', 'N/A'))
                        st.write("**Golf Course:**", selected_tournament.get('Golf Course Name', 'N/A'))
                        st.write("**Location:**", selected_tournament.get('Location', 'N/A'))
                        st.write("**Has Qualifying Rounds:**", '✓' if selected_tournament.get('Has Qualifiers') else '✗')
                    
                    with col2:
                        if selected_tournament.get('Has Qualifiers'):
                            st.write(f"**Number of Qualifying Rounds:** {selected_tournament.get('Qualifier Count', 0)}")
                        else:
                            st.write("**Eligibility:**", selected_tournament.get('Eligibility', 'Not specified'))
                    
                    # Show description if available
                    if selected_tournament.get('Description'):
                        st.subheader("Description")
                        st.write(selected_tournament.get('Description'))
                    
                    # Show detail link if available
                    if selected_tournament.get('Detail URL'):
                        st.markdown(f"[View Tournament Page]({selected_tournament.get('Detail URL')})")
                    
                    # Show qualifying rounds for this tournament
                    related_qualifiers = [
                        q for q in qualifiers 
                        if q.get('Parent Tournament') == selected_tournament.get('Tournament Name')
                    ]
                    
                    if related_qualifiers:
                        st.subheader("Qualifying Rounds")
                        
                        # Create table for this tournament's qualifiers
                        qualifier_data = []
                        for q in related_qualifiers:
                            qualifier_row = {
                                'Date': q.get('Date', 'N/A'),
                                'Course': q.get('Golf Course Name', 'N/A'),
                                'Location': q.get('Location', 'N/A')
                            }
                            
                            qualifier_data.append(qualifier_row)
                        
                        # Show data table for qualifiers
                        st.dataframe(qualifier_data, use_container_width=True)
    
    with tab2:
        if 'tournaments' in st.session_state and st.session_state.tournaments:
            st.subheader("Export Options")
            
            # Export format selection
            export_format = st.radio(
                "Select export format",
                ["CSV", "JSON", "Excel"]
            )
            
            # Export content selection
            export_selection = st.radio(
                "Select data to export",
                ["All data", "Tournaments only", "Qualifying rounds only"]
            )
            
            # Filter data based on selection
            if export_selection == "Tournaments only":
                export_data = [t for t in st.session_state.tournaments if not t.get('Is Qualifier')]
            elif export_selection == "Qualifying rounds only":
                export_data = [t for t in st.session_state.tournaments if t.get('Is Qualifier')]
            else:
                export_data = st.session_state.tournaments
            
            # Custom filename
            filename = st.text_input("Filename", value="golf_tournament_data")
            
            # Create export links
            if export_format == "CSV":
                st.markdown(get_table_download_link(export_data, f"{filename}.csv"), unsafe_allow_html=True)
            elif export_format == "JSON":
                st.markdown(get_json_download_link(export_data, f"{filename}.json"), unsafe_allow_html=True)
            elif export_format == "Excel":
                # Create Excel export
                excel_buffer = io.BytesIO()
                df = pd.DataFrame(export_data)
                df.to_excel(excel_buffer, index=False)
                excel_data = excel_buffer.getvalue()
                b64 = base64.b64encode(excel_data).decode()
                href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{filename}.xlsx">Download Excel file</a>'
                st.markdown(href, unsafe_allow_html=True)
    
    with tab3:
        st.subheader("Help & Documentation")
        
        # Basic instructions
        st.markdown("""
        ### How to Use This Tool
        
        1. **Enter the URL** of a golf tournament page in the sidebar
        2. Click the **"Scrape Tournaments"** button
        3. View the results in two sections:
           - Tournament Data - Main tournaments and their details
           - Qualifying Rounds - Details of all qualifying rounds
        4. Select any tournament to view complete details
        5. Export the data in your preferred format
        
        ### Supported Website Types
        
        This tool specifically supports:
        
        - **Florida State Golf Association (FSGA)** - Automatically detects and optimizes for FSGA tournament listings and qualifying round information
        - **GolfGenius** - Specialized parsing for GolfGenius-powered tournament sites
        - **BlueGolf** - Support for BlueGolf tournament management sites
        - **Generic Golf Sites** - Works with most standard golf tournament websites
        
        ### Example URLs
        
        Try these example URLs:
        
        - FSGA Tournament Details: `https://www.fsga.org/Tournament/Details/6031cb23-72d9-45d8-8b14-1c15808a1ae0`
        - WPGA Registration: `https://wpga-onlineregistration.golfgenius.com/pages/5233160`
        - U.S. Open Local Qualifying: `https://www.amateurgolf.com/amateur-golf-tournaments/11425/U-S--Open-Local-Qualifying-at-Heath--Texas`
        
        ### Troubleshooting
        
        - If no tournaments are found, try:
          - Using a different URL that points to a tournament details page
          - Enabling "Show debug information" in Advanced Options
          
        - If the scraper doesn't find qualifying information:
          - Make sure the tournament actually has qualifying rounds
          - Try accessing the main tournament page rather than a sub-page
        """)

if __name__ == "__main__":
    main()
