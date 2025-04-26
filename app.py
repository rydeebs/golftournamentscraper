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
        return "Qualifier"
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

# Function to scrape detail page
def scrape_detail_page(url, base_url, progress_bar=None):
    """Scrape additional tournament details from the detail page"""
    # Check cache first
    cache_key = hashlib.md5(url.encode()).hexdigest()
    cached_data = load_from_cache(cache_key)
    if cached_data:
        # Update progress bar if provided
        if progress_bar:
            progress_bar.progress(1.0)
        return cached_data
    
    # Ensure URL is absolute
    if not url.startswith(('http://', 'https://')):
        url = urljoin(base_url, url)
    
    # Get HTML content
    html = get_page_html(url)
    if not html:
        return {}
    
    # Parse HTML
    soup = BeautifulSoup(html, 'html.parser')
    
    # Initialize details dictionary
    details = {
        'Description': None,
        'Golf Course Name': None,
        'Location': None,
        'Entry Fee': None,
        'Prize Money': None,
        'Registration Deadline': None,
        'Contact Info': None,
        'Qualifiers': []
    }
    
    # Extract description (look for common description elements)
    description_elements = [
        soup.find('div', class_=lambda c: c and 'description' in c.lower()),
        soup.find('p', class_=lambda c: c and 'desc' in c.lower()),
        soup.find('div', id=lambda i: i and 'description' in i.lower()),
        soup.find('section', class_=lambda c: c and 'info' in c.lower())
    ]
    
    for element in description_elements:
        if element and element.text.strip():
            details['Description'] = element.text.strip()
            break
    
    # Extract golf course name (look for venue information)
    course_elements = [
        soup.find('div', class_=lambda c: c and 'venue' in c.lower()),
        soup.find('h2', class_=lambda c: c and 'course' in c.lower() if c else False),
        soup.find('span', class_=lambda c: c and 'location' in c.lower() if c else False),
        soup.find(lambda tag: tag.name in ['h2', 'h3', 'h4'] and re.search(r'golf\s+(?:club|course)', tag.text.lower(), re.I) if tag.text else False)
    ]
    
    for element in course_elements:
        if element and element.text.strip():
            details['Golf Course Name'] = element.text.strip()
            break
    
    # If no specific course element found, try to extract from text
    if not details['Golf Course Name']:
        course_pattern = r'(?:at|venue|location):\s*([\w\s]+(?:Golf Club|Country Club|Golf Course))'
        match = re.search(course_pattern, soup.text, re.I)
        if match:
            details['Golf Course Name'] = match.group(1).strip()
    
    # Extract location (City, State)
    location_elements = [
        soup.find('div', class_=lambda c: c and 'location' in c.lower()),
        soup.find('span', class_=lambda c: c and 'location' in c.lower()),
        soup.find('address')
    ]
    
    for element in location_elements:
        if element and element.text.strip():
            details['Location'] = extract_location(element.text)
            break
    
    # If no specific location element found, try to extract from text
    if not details['Location']:
        location_pattern = r'(?:in|at)\s+([A-Za-z\s\.]+),\s*([A-Z]{2})'
        match = re.search(location_pattern, soup.text)
        if match:
            city, state = match.groups()
            details['Location'] = f"{city.strip()}, {state.strip()}"
    
    # Extract entry fee
    fee_patterns = [
        r'(?:entry|registration) fee:?\s*\$?(\d+(?:\.\d+)?)',
        r'fee:?\s*\$?(\d+(?:\.\d+)?)',
        r'cost:?\s*\$?(\d+(?:\.\d+)?)',
        r'\$(\d+(?:\.\d+)?)(?:\s*per\s*(?:player|person|entry))',
    ]
    
    for pattern in fee_patterns:
        match = re.search(pattern, soup.text, re.I)
        if match:
            details['Entry Fee'] = f"${match.group(1)}"
            break
    
    # Extract prize money/purse
    prize_patterns = [
        r'(?:prize|purse)(?:\s*money)?:?\s*\$?(\d+(?:,\d+)*(?:\.\d+)?)',
        r'total\s*(?:prize|purse):?\s*\$?(\d+(?:,\d+)*(?:\.\d+)?)',
    ]
    
    for pattern in prize_patterns:
        match = re.search(pattern, soup.text, re.I)
        if match:
            prize = match.group(1)
            # Remove commas
            prize = prize.replace(',', '')
            # Check if it's a reasonable amount (> $100)
            if float(prize) > 100:
                details['Prize Money'] = f"${prize}"
                break
    
    # Extract registration deadline
    deadline_patterns = [
        r'(?:registration|entry)\s*deadline:?\s*([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?,?\s*\d{4})',
        r'(?:register|sign up) by:?\s*([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?,?\s*\d{4})',
        r'deadline:?\s*([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?,?\s*\d{4})',
        r'closes on:?\s*([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?,?\s*\d{4})',
    ]
    
    for pattern in deadline_patterns:
        match = re.search(pattern, soup.text, re.I)
        if match:
            deadline = match.group(1)
            details['Registration Deadline'] = parse_date(deadline)
            break
    
    # Extract contact information
    contact_patterns = [
        r'(?:contact|questions|information):\s*([^\.]+(?:@|phone|tel|email)[^\.]+)',
        r'(?:contact|questions|information)(?:[^\.]+)?(?:contact|email):\s*([^\.]+)',
        r'(?:email|tel|phone):\s*([^\.]+)',
    ]
    
    for pattern in contact_patterns:
        match = re.search(pattern, soup.text, re.I)
        if match:
            details['Contact Info'] = match.group(1).strip()
            break
    
    # Look for qualifying rounds information (specific section or table)
    qualifier_section = None
    qualifier_sections = [
        soup.find('div', id=lambda i: i and 'qualifier' in i.lower() if i else False),
        soup.find('section', class_=lambda c: c and 'qualifier' in c.lower() if c else False),
        soup.find('h2', string=lambda s: s and 'qualif' in s.lower() if s else False),
        soup.find('h3', string=lambda s: s and 'qualif' in s.lower() if s else False),
        soup.find('h4', string=lambda s: s and 'qualif' in s.lower() if s else False)
    ]
    
    for section in qualifier_sections:
        if section:
            qualifier_section = section
            break
    
    # If found a qualifier section header, look at the following elements
    if qualifier_section:
        # Look at next siblings until we find something that looks like qualifier info
        qualifiers = []
        current = qualifier_section.find_next(['p', 'div', 'table', 'ul', 'ol'])
        
        while current and not current.find_parent('header'):
            qualifier_text = current.get_text()
            
            # Extract date from qualifier info
            date_match = re.search(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:st|nd|rd|th)?,? \d{4}|\d{1,2}/\d{1,2}/\d{4}', qualifier_text)
            
            # Extract location from qualifier info
            location_match = re.search(r'([A-Za-z\s\.]+),\s*([A-Z]{2}|[A-Za-z\s]+)', qualifier_text)
            
            # Extract course from qualifier info
            course_match = re.search(r'at\s+([\w\s]+(?:Golf Club|Country Club|Golf Course))', qualifier_text, re.I)
            
            if date_match or location_match or course_match:
                qualifier = {
                    'Date': date_match.group(0) if date_match else None,
                    'Location': f"{location_match.group(1).strip()}, {location_match.group(2).strip()}" if location_match else None,
                    'Golf Course Name': course_match.group(1).strip() if course_match else None
                }
                
                # Parse the date if found
                if qualifier['Date']:
                    qualifier['Date'] = parse_date(qualifier['Date'])
                
                qualifiers.append(qualifier)
            
            current = current.find_next(['p', 'div', 'table', 'ul', 'ol'])
            # Break if we've moved too far away
            if current and current.name in ['h1', 'h2', 'h3', 'footer']:
                break
        
        details['Qualifiers'] = qualifiers
    
    # If we haven't found any qualifiers by section, look for qualifying keywords
    if not details['Qualifiers']:
        qualifier_phrases = soup.find_all(string=lambda text: text and re.search(r'qualif(?:y|ier|ying|ication)', text.lower()) if text else False)
        
        for phrase in qualifier_phrases:
            # Look at surrounding content
            surrounding = phrase.parent
            
            # Get the surrounding text
            surrounding_text = surrounding.get_text()
            
            # Extract date
            date_match = re.search(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:st|nd|rd|th)?,? \d{4}|\d{1,2}/\d{1,2}/\d{4}', surrounding_text)
            
            # Extract location
            location_match = re.search(r'([A-Za-z\s\.]+),\s*([A-Z]{2}|[A-Za-z\s]+)', surrounding_text)
            
            # Extract course
            course_match = re.search(r'at\s+([\w\s]+(?:Golf Club|Country Club|Golf Course))', surrounding_text, re.I)
            
            if date_match or location_match or course_match:
                qualifier = {
                    'Date': date_match.group(0) if date_match else None,
                    'Location': f"{location_match.group(1).strip()}, {location_match.group(2).strip()}" if location_match else None,
                    'Golf Course Name': course_match.group(1).strip() if course_match else None
                }
                
                # Parse the date if found
                if qualifier['Date']:
                    qualifier['Date'] = parse_date(qualifier['Date'])
                
                # Check if this qualifier is different from existing ones
                is_new = True
                for existing in details['Qualifiers']:
                    if existing.get('Date') == qualifier.get('Date') and existing.get('Location') == qualifier.get('Location'):
                        is_new = False
                        break
                
                if is_new:
                    details['Qualifiers'].append(qualifier)
    
    # Save to cache
    save_to_cache(cache_key, details)
    
    # Update progress bar if provided
    if progress_bar:
        progress_bar.progress(1.0)
    
    return details

# Function to scrape tournament data concurrently
def scrape_tournament_details_concurrently(tournaments, base_url, max_workers=5, max_details=None):
    """Scrape detail pages concurrently using ThreadPoolExecutor"""
    # Filter tournaments with detail URLs
    tournaments_with_details = [t for t in tournaments if t.get('Detail URL')]
    
    # Limit by max_details if specified
    if max_details and max_details < len(tournaments_with_details):
        tournaments_with_details = tournaments_with_details[:max_details]
    
    # Create progress container
    progress_text = st.empty()
    progress_bar = st.progress(0.0)
    
    # Define function to process a single tournament
    def process_tournament(index, tournament):
        progress_text.text(f"Scraping details for {tournament['Tournament Name']} ({index+1}/{len(tournaments_with_details)})")
        detail_data = scrape_detail_page(tournament['Detail URL'], base_url)
        return tournament, detail_data
    
    # Use ThreadPoolExecutor to process tournaments concurrently
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_index = {
            executor.submit(process_tournament, i, t): i 
            for i, t in enumerate(tournaments_with_details)
        }
        
        # Process results as they complete
        for future in as_completed(future_to_index):
            i = future_to_index[future]
            progress = (i + 1) / len(tournaments_with_details)
            progress_bar.progress(progress)
    
    # Get results
    results = []
    for future in future_to_index:
        try:
            tournament, detail_data = future.result()
            
            # Update tournament data with detail information
            for key, value in detail_data.items():
                if key != 'Qualifiers' and value:
                    tournament[key] = value
            
            # Process qualifiers if found
            if 'Qualifiers' in detail_data and detail_data['Qualifiers']:
                for qualifier in detail_data['Qualifiers']:
                    if all(value is None for value in qualifier.values()):
                        continue  # Skip empty qualifiers
                        
                    qualifier_data = {
                        'Tournament Name': f"{tournament['Tournament Name']} - Qualifier",
                        'Date': qualifier.get('Date', tournament.get('Date')),
                        'Golf Course Name': qualifier.get('Golf Course Name', tournament.get('Golf Course Name')),
                        'Location': qualifier.get('Location', tournament.get('Location')),
                        'Tournament Type': 'Qualifier',
                        'Is Qualifier': True,
                        'Detail URL': tournament.get('Detail URL'),  # Use main tournament detail link
                        'Parent Tournament': tournament['Tournament Name'],
                        'Parent Tournament ID': generate_tournament_id(tournament)
                    }
                    results.append(qualifier_data)
            
            results.append(tournament)
        except Exception as e:
            logger.error(f"Error processing tournament: {str(e)}")
    
    # Reset progress
    progress_bar.empty()
    progress_text.empty()
    
    return results

# Main function to scrape tournaments
def scrape_tournaments(url, max_details=None, show_progress=True, use_threading=True, max_workers=5):
    """Main function to scrape tournament data from a URL with optional detail page scraping"""
    # Check cache first
    cache_key = f"tournaments_{hashlib.md5(url.encode()).hexdigest()}"
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
    
    # Strategy 4: Look for event cards or blocks
    if len(tournament_elements) < 3:
        # Look for common event card patterns
        card_patterns = [
            ['div', {'class': lambda c: c and ('card' in c.lower())}],
            ['div', {'class': lambda c: c and ('event' in c.lower())}],
            ['div', {'class': lambda c: c and ('block' in c.lower())}],
            ['div', {'data-type': lambda d: d and ('event' in d.lower())}]
        ]
        
        for selector, attrs in card_patterns:
            elements = soup.find_all(selector, attrs)
            if elements:
                tournament_elements.extend(elements)
                if len(tournament_elements) > 10:
                    break
    
    # Prepare data structure
    tournaments = []
    
    # Set up progress tracking
    if show_progress:
        progress_text = st.empty()
        progress_bar = st.progress(0.0)
        progress_text.text("Finding tournament elements...")
    else:
        progress_text = None
        progress_bar = None
    
    # Get total count for progress calculation
    total_elements = len(tournament_elements)
    
    # Loop through tournament elements
    for i, element in enumerate(tournament_elements):
        # Update progress
        if show_progress and total_elements > 0:
            progress = i / total_elements
            progress_bar.progress(progress)
            progress_text.text(f"Processing tournament {i+1} of {total_elements}...")
        
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
            
        # Extract golf course
        course = extract_golf_course(element.get_text())
        
        # Parse date range if present
        date_range = None
        if date:
            date_range = parse_date_range(date_match.group(0))
        
        # Initialize tournament data
        tournament_data = {
            'Tournament Name': tournament_name,
            'Date': date,
            'Golf Course Name': course,
            'Location': location,
            'Tournament Type': determine_tournament_type(tournament_name),
            'Is Qualifier': is_qualifier(tournament_name),
            'Detail URL': detail_link,
            'Tournament ID': generate_tournament_id({'Tournament Name': tournament_name, 'Date': date, 'Location': location})
        }
        
        # Add date range info if available
        if date_range and date_range.get('days'):
            tournament_data['Start Date'] = date_range.get('start_date')
            tournament_data['End Date'] = date_range.get('end_date')
            tournament_data['Days'] = date_range.get('days')
        
        # Add tournament to list
        tournaments.append(tournament_data)
    
    # Complete initial progress
    if show_progress:
        progress_bar.progress(1.0)
        progress_text.text(f"Found {len(tournaments)} tournaments")
    
    # Process detail pages if needed
    if max_details and detail_links:
        if show_progress:
            progress_text.text("Scraping tournament details...")
        
        if use_threading:
            tournaments = scrape_tournament_details_concurrently(
                tournaments, url, max_workers=max_workers, max_details=max_details
            )
        else:
            # Traditional sequential processing
            for i, tournament in enumerate(tournaments):
                if tournament.get('Detail URL') and (max_details is None or i < max_details):
                    if show_progress:
                        progress_text.text(f"Scraping details for {tournament['Tournament Name']}...")
                        detail_progress = st.empty()
                        detail_progress.progress(0.0)
                    else:
                        detail_progress = None
                        
                    detail_data = scrape_detail_page(tournament['Detail URL'], url, detail_progress)
                    
                    # Update tournament data with detail information
                    for key, value in detail_data.items():
                        if key != 'Qualifiers' and value:
                            tournament[key] = value
                    
                    # Process qualifiers if found
                    if 'Qualifiers' in detail_data and detail_data['Qualifiers']:
                        for qualifier in detail_data['Qualifiers']:
                            if all(value is None for value in qualifier.values()):
                                continue  # Skip empty qualifiers
                                
                            qualifier_data = {
                                'Tournament Name': f"{tournament['Tournament Name']} - Qualifier",
                                'Date': qualifier.get('Date', tournament.get('Date')),
                                'Golf Course Name': qualifier.get('Golf Course Name', tournament.get('Golf Course Name')),
                                'Location': qualifier.get('Location', tournament.get('Location')),
                                'Tournament Type': 'Qualifier',
                                'Is Qualifier': True,
                                'Detail URL': tournament.get('Detail URL'),  # Use main tournament detail link
                                'Parent Tournament': tournament['Tournament Name'],
                                'Parent Tournament ID': tournament.get('Tournament ID')
                            }
                            tournaments.append(qualifier_data)
    
    # Save to cache
    save_to_cache(cache_key, tournaments)
    
    # Complete progress
    if show_progress:
        qualifier_count = sum(1 for t in tournaments if t.get('Is Qualifier'))
        progress_text.text(f"Found {len(tournaments)} tournaments (including {qualifier_count} qualifiers)")
    
    return tournaments

# Function to analyze tournament data
def analyze_tournament_data(tournaments):
    """Analyze tournament data for insights"""
    if not tournaments:
        return {}
    
    # Convert to DataFrame for easier analysis
    df = pd.DataFrame(tournaments)
    
    # Initialize results
    analysis = {
        'total_count': len(df),
        'qualifier_count': sum(df.get('Is Qualifier', False)),
        'tournament_types': {},
        'months': {},
        'locations': {},
        'courses': {},
        'avg_tournament_length': None
    }
    
    # Count by tournament type
    if 'Tournament Type' in df.columns:
        type_counts = df['Tournament Type'].value_counts().to_dict()
        analysis['tournament_types'] = type_counts
    
    # Count by month if dates available
    if 'Date' in df.columns:
        # Convert to datetime
        valid_dates = df[df['Date'].notna() & df['Date'].str.contains('-')]
        if not valid_dates.empty:
            valid_dates['Month'] = pd.to_datetime(valid_dates['Date']).dt.month
            valid_dates['Month_Name'] = pd.to_datetime(valid_dates['Date']).dt.strftime('%B')
            month_counts = valid_dates['Month_Name'].value_counts().to_dict()
            
            # Sort by month number
            month_order = {
                'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
                'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12
            }
            analysis['months'] = {k: month_counts[k] for k in sorted(month_counts.keys(), key=lambda x: month_order.get(x, 13))}
    
    # Count by location
    if 'Location' in df.columns:
        location_counts = df['Location'].value_counts().head(10).to_dict()
        analysis['locations'] = location_counts
    
    # Count by course
    if 'Golf Course Name' in df.columns:
        course_counts = df['Golf Course Name'].value_counts().head(10).to_dict()
        analysis['courses'] = course_counts
    
    # Calculate average tournament length
    if 'Days' in df.columns:
        days = df['Days'].dropna()
        if not days.empty:
            analysis['avg_tournament_length'] = round(days.mean(), 1)
    
    return analysis

# Function to create tournament network visualization
def create_tournament_network(tournaments):
    """Create network visualization of tournaments and qualifiers"""
    # Create nodes for tournaments
    nodes = []
    edges = []
    
    # Create mapping for tournament IDs to indices
    tournament_map = {}
    
    for i, tournament in enumerate(tournaments):
        # Skip if no ID
        if 'Tournament ID' not in tournament:
            continue
        
        # Add to map
        tournament_map[tournament['Tournament ID']] = i
        
        # Create node
        node = {
            'id': i,
            'name': tournament['Tournament Name'],
            'type': tournament.get('Tournament Type', 'Unknown'),
            'qualifier': tournament.get('Is Qualifier', False)
        }
        
        nodes.append(node)
        
        # Create edge if this is a qualifier
        if tournament.get('Is Qualifier') and tournament.get('Parent Tournament ID'):
            parent_id = tournament.get('Parent Tournament ID')
            
            # Check if parent is in map
            if parent_id in tournament_map:
                edge = {
                    'source': i,
                    'target': tournament_map[parent_id],
                }
                
                edges.append(edge)
    
    return {'nodes': nodes, 'edges': edges}

# Class for tournament site profile detection
class TournamentSiteDetector:
    """Class to detect and handle different tournament site profiles"""
    
    # Predefined profiles
    profiles = {
        'fsga': {
            'name': 'Florida State Golf Association',
            'patterns': ['fsga.org', 'florida state golf', 'fsga'],
            'selectors': {
                'tournaments': [
                    {'selector': 'table.tournament-results tr', 'type': 'table_row'},
                    {'selector': 'div.tournament-item', 'type': 'card'}
                ],
                'name': [
                    {'selector': 'td:first-child a', 'attribute': 'text'},
                    {'selector': 'h3.tournament-name', 'attribute': 'text'}
                ],
                'date': [
                    {'selector': 'td:nth-child(2)', 'attribute': 'text'},
                    {'selector': 'div.tournament-date', 'attribute': 'text'}
                ],
                'location': [
                    {'selector': 'td:nth-child(3)', 'attribute': 'text'},
                    {'selector': 'div.tournament-location', 'attribute': 'text'}
                ]
            }
        },
        'usga': {
            'name': 'United States Golf Association',
            'patterns': ['usga.org', 'us golf', 'usga'],
            'selectors': {
                'tournaments': [
                    {'selector': 'table.championships-table tr', 'type': 'table_row'},
                    {'selector': 'div.championship-card', 'type': 'card'}
                ],
                'name': [
                    {'selector': 'td.championship-name a', 'attribute': 'text'},
                    {'selector': 'h3.championship-title', 'attribute': 'text'}
                ],
                'date': [
                    {'selector': 'td.championship-date', 'attribute': 'text'},
                    {'selector': 'div.championship-dates', 'attribute': 'text'}
                ],
                'location': [
                    {'selector': 'td.championship-location', 'attribute': 'text'},
                    {'selector': 'div.championship-venue', 'attribute': 'text'}
                ]
            }
        },
        'pga': {
            'name': 'Professional Golfers Association',
            'patterns': ['pga.com', 'professional golfers', 'pga'],
            'selectors': {
                'tournaments': [
                    {'selector': 'table.tournament-schedule tr', 'type': 'table_row'},
                    {'selector': 'div.tournament-card', 'type': 'card'}
                ],
                'name': [
                    {'selector': 'td.tournament-name a', 'attribute': 'text'},
                    {'selector': 'h3.tournament-title', 'attribute': 'text'}
                ],
                'date': [
                    {'selector': 'td.tournament-date', 'attribute': 'text'},
                    {'selector': 'div.tournament-dates', 'attribute': 'text'}
                ],
                'location': [
                    {'selector': 'td.tournament-location', 'attribute': 'text'},
                    {'selector': 'div.tournament-venue', 'attribute': 'text'}
                ]
            }
        }
    }
    
    @classmethod
    def detect_profile(cls, url, html=None):
        """Detect the profile based on URL and optionally HTML content"""
        # Check URL against known patterns
        for profile_id, profile in cls.profiles.items():
            for pattern in profile['patterns']:
                if pattern in url.lower():
                    return profile_id
        
        # If HTML provided, try more advanced detection
        if html:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Check for specific indicators in HTML
            for profile_id, profile in cls.profiles.items():
                for tournament_selector in profile['selectors']['tournaments']:
                    elements = soup.select(tournament_selector['selector'])
                    if len(elements) > 3:  # Found significant number of matches
                        return profile_id
        
        # No match found
        return None
    
    @classmethod
    def get_profile(cls, profile_id):
        """Get profile by ID"""
        return cls.profiles.get(profile_id)
    
    @classmethod
    def get_all_profiles(cls):
        """Get all profiles"""
        return cls.profiles
    
    @classmethod
    def scrape_with_profile(cls, url, profile_id, max_details=None):
        """Scrape using a specific profile"""
        profile = cls.get_profile(profile_id)
        if not profile:
            return None
        
        # Get HTML
        html = get_page_html(url)
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'html.parser')
        tournaments = []
        
        # Extract tournaments based on profile selectors
        for tournament_selector in profile['selectors']['tournaments']:
            elements = soup.select(tournament_selector['selector'])
            
            # Skip header row for tables
            if tournament_selector['type'] == 'table_row' and elements:
                elements = elements[1:]
            
            for element in elements:
                tournament = {}
                
                # Extract name
                for name_selector in profile['selectors']['name']:
                    name_elem = element.select_one(name_selector['selector'])
                    if name_elem:
                        tournament['Tournament Name'] = name_elem.get_text().strip()
                        break
                
                # Skip if no name found
                if 'Tournament Name' not in tournament:
                    continue
                
                # Extract date
                for date_selector in profile['selectors']['date']:
                    date_elem = element.select_one(date_selector['selector'])
                    if date_elem:
                        date_text = date_elem.get_text().strip()
                        tournament['Date'] = parse_date(date_text)
                        
                        # Parse date range
                        date_range = parse_date_range(date_text)
                        if date_range.get('days'):
                            tournament['Start Date'] = date_range.get('start_date')
                            tournament['End Date'] = date_range.get('end_date')
                            tournament['Days'] = date_range.get('days')
                        
                        break
                
                # Extract location
                for location_selector in profile['selectors']['location']:
                    location_elem = element.select_one(location_selector['selector'])
                    if location_elem:
                        tournament['Location'] = extract_location(location_elem.get_text().strip())
                        break
                
                # Find link
                link_elem = element.select_one('a')
                if link_elem and 'href' in link_elem.attrs:
                    link = link_elem['href']
                    # Make absolute URL
                    if not link.startswith(('http://', 'https://')):
                        link = urljoin(url, link)
                    tournament['Detail URL'] = link
                
                # Add other fields
                tournament['Tournament Type'] = determine_tournament_type(tournament.get('Tournament Name', ''))
                tournament['Is Qualifier'] = is_qualifier(tournament.get('Tournament Name', ''))
                tournament['Tournament ID'] = generate_tournament_id(tournament)
                
                tournaments.append(tournament)
        
        # Process detail pages if needed
        if max_details and tournaments:
            processed_tournaments = []
            
            for i, tournament in enumerate(tournaments):
                if tournament.get('Detail URL') and (max_details is None or i < max_details):
                    detail_data = scrape_detail_page(tournament['Detail URL'], url)
                    
                    # Update tournament data with detail information
                    for key, value in detail_data.items():
                        if key != 'Qualifiers' and value:
                            tournament[key] = value
                    
                    # Process qualifiers if found
                    if 'Qualifiers' in detail_data and detail_data['Qualifiers']:
                        for qualifier in detail_data['Qualifiers']:
                            if all(value is None for value in qualifier.values()):
                                continue  # Skip empty qualifiers
                                
                            qualifier_data = {
                                'Tournament Name': f"{tournament['Tournament Name']} - Qualifier",
                                'Date': qualifier.get('Date', tournament.get('Date')),
                                'Golf Course Name': qualifier.get('Golf Course Name', tournament.get('Golf Course Name')),
                                'Location': qualifier.get('Location', tournament.get('Location')),
                                'Tournament Type': 'Qualifier',
                                'Is Qualifier': True,
                                'Detail URL': tournament.get('Detail URL'),
                                'Parent Tournament': tournament['Tournament Name'],
                                'Parent Tournament ID': tournament.get('Tournament ID')
                            }
                            processed_tournaments.append(qualifier_data)
                
                processed_tournaments.append(tournament)
            
            return processed_tournaments
        
        return tournaments

# Streamlit UI
def main():
    st.set_page_config(page_title="Enhanced Golf Tournament Scraper", layout="wide")
    
    st.title("Enhanced Golf Tournament Scraper")
    
    st.markdown("""
    ## Extract tournament and qualifier data from golf association websites
    
    This tool helps you gather information about tournaments and qualifying rounds from various golf association websites.
    Simply enter the URL of a tournament page and click "Scrape Tournaments".
    """)
    
    # Sidebar for configuration
    st.sidebar.title("Configuration")
    
    # Input for URL
    url = st.sidebar.text_input(
        "Tournament Page URL",
        value="https://www.fsga.org/TournamentResults"
    )
    
    # Site profile detection
    if url:
        profile_id = TournamentSiteDetector.detect_profile(url)
        if profile_id:
            profile = TournamentSiteDetector.get_profile(profile_id)
            st.sidebar.success(f"Detected site profile: {profile['name']}")
            use_profile = st.sidebar.checkbox("Use optimized site profile?", value=True)
        else:
            use_profile = False
    else:
        use_profile = False
        profile_id = None
    
    # Advanced options
    with st.sidebar.expander("Advanced Options"):
        max_details = st.number_input("Maximum detail pages to scrape (0 = None)", min_value=0, value=10, step=1)
        max_details = None if max_details == 0 else max_details
        
        use_threading = st.checkbox("Use multi-threading for faster scraping", value=True)
        max_workers = st.slider("Maximum concurrent workers", min_value=1, max_value=10, value=5)
        
        cache_duration = st.number_input("Cache duration (hours)", min_value=1, max_value=72, value=24)
        
        show_debug = st.checkbox("Show debug information", value=False)
        
        if show_debug:
            logging.getLogger().setLevel(logging.DEBUG)
        else:
            logging.getLogger().setLevel(logging.INFO)
    
    # Create tabs for different functionality
    tab1, tab2, tab3, tab4 = st.tabs(["Scraper", "Visualizations", "Export", "Site Profiles"])
    
    with tab1:
        # Button to start scraping
        if st.button("Scrape Tournaments", type="primary"):
            if url:
                with st.spinner('Scraping data...'):
                    try:
                        # Determine scraping method
                        if use_profile and profile_id:
                            # Use profile-specific scraping
                            tournaments = TournamentSiteDetector.scrape_with_profile(url, profile_id, max_details)
                        else:
                            # Use generic scraping
                            tournaments = scrape_tournaments(
                                url, 
                                max_details=max_details, 
                                show_progress=True,
                                use_threading=use_threading,
                                max_workers=max_workers
                            )
                        
                        if not tournaments:
                            st.error("No tournament data found. The website structure might not be supported.")
                        else:
                            # Store the data in session state for filtering
                            st.session_state.tournaments = tournaments
                            st.session_state.filtered_tournaments = tournaments
                            
                            # Display results
                            st.success(f"Found {len(tournaments)} tournaments")
                            
                            # Count qualifiers
                            qualifier_count = sum(1 for t in tournaments if t.get('Is Qualifier'))
                            st.info(f"Including {qualifier_count} qualifying rounds")
                            
                            # Perform analysis
                            analysis = analyze_tournament_data(tournaments)
                            st.session_state.analysis = analysis
                            
                    except Exception as e:
                        st.error(f"An error occurred during scraping: {str(e)}")
                        if show_debug:
                            st.exception(e)
            else:
                st.error("Please enter a valid URL")
        
        # Initialize session state if not exists
        if 'tournaments' not in st.session_state:
            st.session_state.tournaments = []
        
        if 'filtered_tournaments' not in st.session_state:
            st.session_state.filtered_tournaments = []
        
        if 'analysis' not in st.session_state:
            st.session_state.analysis = {}
        
        # Only show filters and results if we have tournaments
        if st.session_state.tournaments:
            # Display filter options in separate columns
            st.subheader("Filter Results")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Filter by tournament type
                tournament_types = ["All"] + sorted(list(set(t.get('Tournament Type') for t in st.session_state.tournaments if t.get('Tournament Type'))))
                selected_type = st.selectbox("Tournament Type", tournament_types)
            
            with col2:
                # Filter by qualifier status
                qualifier_status = st.radio("Qualifier Status", ["All", "Qualifiers Only", "Non-Qualifiers Only"])
            
            with col3:
                # Filter by text search
                search_text = st.text_input("Search (tournament name, location, course)")
                
                # Date range filter
                date_filter = st.checkbox("Filter by date range")
            
            # Date range selector (only show if date filter is checked)
            if date_filter:
                col1, col2 = st.columns(2)
                with col1:
                    start_date = st.date_input("Start date", value=None)
                with col2:
                    end_date = st.date_input("End date", value=None)
            else:
                start_date = None
                end_date = None
            
            # Apply filters
            filtered_tournaments = st.session_state.tournaments.copy()
            
            # Filter by tournament type
            if selected_type != "All":
                filtered_tournaments = [t for t in filtered_tournaments if t.get('Tournament Type') == selected_type]
            
            # Filter by qualifier status
            if qualifier_status == "Qualifiers Only":
                filtered_tournaments = [t for t in filtered_tournaments if t.get('Is Qualifier')]
            elif qualifier_status == "Non-Qualifiers Only":
                filtered_tournaments = [t for t in filtered_tournaments if not t.get('Is Qualifier')]
            
            # Filter by search text
            if search_text:
                search_text = search_text.lower()
                filtered_tournaments = [t for t in filtered_tournaments if 
                                      (t.get('Tournament Name') and search_text in t.get('Tournament Name').lower()) or
                                      (t.get('Location') and search_text in t.get('Location').lower()) or
                                      (t.get('Golf Course Name') and search_text in t.get('Golf Course Name').lower()) or
                                      (t.get('Description') and search_text in t.get('Description').lower())]
            
            # Filter by date range
            if date_filter and start_date and end_date:
                # Convert to datetime objects for comparison
                start_date_str = start_date.strftime('%Y-%m-%d')
                end_date_str = end_date.strftime('%Y-%m-%d')
                
                # Filter tournaments with valid dates
                filtered_tournaments = [t for t in filtered_tournaments if 
                                      t.get('Date') and 
                                      t.get('Date') != 'TBD' and
                                      '-' in t.get('Date', '') and
                                      start_date_str <= t.get('Date') <= end_date_str]
            
            # Store filtered tournaments in session state
            st.session_state.filtered_tournaments = filtered_tournaments
            
            # Display results
            st.subheader("Tournament Data")
            
            # Create a more readable display table
            display_data = []
            for t in filtered_tournaments:
                display_row = {
                    'Name': t.get('Tournament Name', ''),
                    'Date': t.get('Date', 'N/A'),
                    'Course': t.get('Golf Course Name', 'N/A'),
                    'Location': t.get('Location', 'N/A'),
                    'Type': t.get('Tournament Type', ''),
                    'Is Qualifier': '✓' if t.get('Is Qualifier') else ''
                }
                
                # Add parent tournament for qualifiers
                if t.get('Parent Tournament'):
                    display_row['Parent Tournament'] = t.get('Parent Tournament')
                
                # Add days for multi-day tournaments
                if t.get('Days') and t.get('Days') > 1:
                    display_row['Days'] = t.get('Days')
                
                # Add entry fee if available
                if t.get('Entry Fee'):
                    display_row['Entry Fee'] = t.get('Entry Fee')
                
                display_data.append(display_row)
            
            # Show data table
            st.dataframe(display_data, use_container_width=True)
            
            # Show download link (will be moved to Export tab)
            st.markdown(get_table_download_link(filtered_tournaments), unsafe_allow_html=True)
            
            # Show tournament details on click
            if st.checkbox("Show detailed information for selected tournament"):
                selected_tournament_name = st.selectbox(
                    "Select a tournament", 
                    options=[t.get('Tournament Name', 'Unknown') for t in filtered_tournaments]
                )
                
                # Find the selected tournament
                selected_tournament = next(
                    (t for t in filtered_tournaments if t.get('Tournament Name') == selected_tournament_name), 
                    None
                )
                
                if selected_tournament:
                    st.subheader(f"Details for {selected_tournament_name}")
                    
                    # Display tournament details
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Date:**", selected_tournament.get('Date', 'N/A'))
                        st.write("**Golf Course:**", selected_tournament.get('Golf Course Name', 'N/A'))
                        st.write("**Location:**", selected_tournament.get('Location', 'N/A'))
                        st.write("**Tournament Type:**", selected_tournament.get('Tournament Type', 'N/A'))
                        
                        if selected_tournament.get('Is Qualifier'):
                            st.write("**Parent Tournament:**", selected_tournament.get('Parent Tournament', 'N/A'))
                        
                        if selected_tournament.get('Days'):
                            st.write("**Duration:**", f"{selected_tournament.get('Days')} days")
                            
                    with col2:
                        if selected_tournament.get('Entry Fee'):
                            st.write("**Entry Fee:**", selected_tournament.get('Entry Fee'))
                        
                        if selected_tournament.get('Prize Money'):
                            st.write("**Prize Money:**", selected_tournament.get('Prize Money'))
                        
                        if selected_tournament.get('Registration Deadline'):
                            st.write("**Registration Deadline:**", selected_tournament.get('Registration Deadline'))
                        
                        if selected_tournament.get('Contact Info'):
                            st.write("**Contact:**", selected_tournament.get('Contact Info'))
                    
                    # Show description if available
                    if selected_tournament.get('Description'):
                        st.subheader("Description")
                        st.write(selected_tournament.get('Description'))
                    
                    # Show detail link if available
                    if selected_tournament.get('Detail URL'):
                        st.markdown(f"[View Tournament Page]({selected_tournament.get('Detail URL')})")
                    
                    # Show qualifiers if this is a parent tournament
                    qualifiers = [
                        t for t in filtered_tournaments 
                        if t.get('Parent Tournament ID') == selected_tournament.get('Tournament ID')
                    ]
                    
                    if qualifiers:
                        st.subheader("Qualifying Rounds")
                        for q in qualifiers:
                            st.write(f"- **{q.get('Tournament Name')}** - {q.get('Date', 'TBD')} at {q.get('Golf Course Name', 'TBD')}")
    
    with tab2:
        if 'tournaments' in st.session_state and st.session_state.tournaments:
            st.subheader("Tournament Analysis")
            
            # Show statistics if available
            if 'analysis' in st.session_state and st.session_state.analysis:
                analysis = st.session_state.analysis
                
                # Create columns for statistics
                col1, col2 = st.columns(2)
                
                with col1:
                    # Count by tournament type
                    if analysis.get('tournament_types'):
                        st.write("**Tournament Types:**")
                        for t_type, count in sorted(analysis['tournament_types'].items()):
                            st.write(f"- {t_type}: {count}")
                    
                    # Count by locations
                    if analysis.get('locations'):
                        st.write("**Top Locations:**")
                        for location, count in sorted(analysis['locations'].items(), key=lambda x: x[1], reverse=True)[:5]:
                            st.write(f"- {location}: {count}")
                
                with col2:
                    # Count by month 
                    if analysis.get('months'):
                        st.write("**Tournaments by Month:**")
                        for month_name, count in analysis['months'].items():
                            st.write(f"- {month_name}: {count}")
                    
                    # Average tournament length
                    if analysis.get('avg_tournament_length'):
                        st.write(f"**Average Tournament Length:** {analysis['avg_tournament_length']} days")
                
                # Create tournament calendar visualization
                st.subheader("Tournament Calendar")
                
                # Filter tournaments with valid dates
                tournaments_with_dates = [
                    t for t in st.session_state.tournaments 
                    if t.get('Date') and t.get('Date') != 'TBD' and '-' in t.get('Date', '')
                ]
                
                if tournaments_with_dates:
                    # Convert to DataFrame for easier visualization
                    df = pd.DataFrame(tournaments_with_dates)
                    df['Date'] = pd.to_datetime(df['Date'])
                    df['Month'] = df['Date'].dt.month
                    df['Year'] = df['Date'].dt.year
                    
                    # Group by month
                    monthly_counts = df.groupby(['Year', 'Month']).size().reset_index(name='Count')
                    
                    # Create chart data for display
                    chart_data = pd.DataFrame({
                        'Month': monthly_counts.apply(lambda x: f"{x['Year']}-{x['Month']:02d}", axis=1),
                        'Tournaments': monthly_counts['Count']
                    })
                    
                    # Display the chart
                    st.bar_chart(chart_data.set_index('Month'))
                else:
                    st.info("No tournaments with valid dates found for calendar visualization.")
                
                # Tournament network visualization
                st.subheader("Tournament & Qualifier Network")
                
                # Filter to include only tournaments with qualifiers
                tournaments_with_qualifiers = [
                    t for t in st.session_state.tournaments 
                    if any(q.get('Parent Tournament ID') == t.get('Tournament ID') 
                          for q in st.session_state.tournaments)
                ]
                
                # Add the qualifiers to the list
                qualifier_ids = [
                    q.get('Parent Tournament ID') for q in st.session_state.tournaments 
                    if q.get('Parent Tournament ID')
                ]
                
                network_tournaments = tournaments_with_qualifiers + [
                    q for q in st.session_state.tournaments 
                    if q.get('Parent Tournament ID') in [t.get('Tournament ID') for t in tournaments_with_qualifiers]
                ]
                
                if network_tournaments:
                    # Display network visualization using HTML/JavaScript
                    st.info("Network visualization shows connections between tournaments and their qualifying rounds.")
                    
                    # Create simple HTML representation
                    network_data = create_tournament_network(network_tournaments)
                    
                    # Display tournament count in network
                    st.write(f"**Showing network for {len(network_data['nodes'])} tournaments and qualifiers**")
                    
                    # Display sample of connections
                    st.write("**Sample connections:**")
                    edge_count = min(5, len(network_data['edges']))
                    for i in range(edge_count):
                        edge = network_data['edges'][i]
                        source = network_data['nodes'][edge['source']]['name']
                        target = network_data['nodes'][edge['target']]['name']
                        st.write(f"- {source} → {target}")
                else:
                    st.info("No tournaments with qualifying rounds found for network visualization.")
    
    with tab3:
        if 'tournaments' in st.session_state and st.session_state.tournaments:
            st.subheader("Export Options")
            
            # Export format selection
            export_format = st.radio(
                "Select export format",
                ["CSV", "JSON", "Excel"]
            )
            
            # Export contents selection
            export_selection = st.radio(
                "Select data to export",
                ["All tournaments", "Filtered tournaments only", "Selected tournaments"]
            )
            
            if export_selection == "Selected tournaments":
                # Multi-select for tournaments
                selected_tournaments = st.multiselect(
                    "Select tournaments to export",
                    options=[t.get('Tournament Name', 'Unknown') for t in st.session_state.tournaments]
                )
                
                # Filter to selected tournaments
                export_data = [
                    t for t in st.session_state.tournaments 
                    if t.get('Tournament Name') in selected_tournaments
                ]
            elif export_selection == "Filtered tournaments only":
                export_data = st.session_state.filtered_tournaments
            else:
                export_data = st.session_state.tournaments
            
            # Custom filename
            filename = st.text_input("Filename", value="tournament_data")
            
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
            
            # Additional export options
            st.subheader("Additional Export Options")
            
            # Export for calendar integration
            if st.checkbox("Generate iCalendar (ICS) file for calendar integration"):
                # Create iCalendar data
                from icalendar import Calendar, Event
                import pytz
                from datetime import datetime, timedelta
                
                cal = Calendar()
                cal.add('prodid', '-//Enhanced Golf Tournament Scraper//EN')
                cal.add('version', '2.0')
                
                # Add tournaments to calendar
                added_count = 0
                for tournament in export_data:
                    if not tournament.get('Date') or tournament.get('Date') == 'TBD' or '-' not in tournament.get('Date', ''):
                        continue
                    
                    event = Event()
                    event.add('summary', tournament.get('Tournament Name', 'Golf Tournament'))
                    
                    # Parse date
                    start_date = datetime.strptime(tournament.get('Date'), '%Y-%m-%d')
                    
                    # Set end date (use Days if available, otherwise default to 1 day)
                    days = tournament.get('Days', 1)
                    if not days:
                        days = 1
                    end_date = start_date + timedelta(days=days)
                    
                    event.add('dtstart', start_date.date())
                    event.add('dtend', end_date.date())
                    
                    # Add location if available
                    location_parts = []
                    if tournament.get('Golf Course Name'):
                        location_parts.append(tournament.get('Golf Course Name'))
                    if tournament.get('Location'):
                        location_parts.append(tournament.get('Location'))
                    
                    if location_parts:
                        event.add('location', ', '.join(location_parts))
                    
                    # Add description if available
                    description_parts = []
                    if tournament.get('Description'):
                        description_parts.append(tournament.get('Description'))
                    if tournament.get('Entry Fee'):
                        description_parts.append(f"Entry Fee: {tournament.get('Entry Fee')}")
                    if tournament.get('Prize Money'):
                        description_parts.append(f"Prize Money: {tournament.get('Prize Money')}")
                    if tournament.get('Contact Info'):
                        description_parts.append(f"Contact: {tournament.get('Contact Info')}")
                    if tournament.get('Detail URL'):
                        description_parts.append(f"More Info: {tournament.get('Detail URL')}")
                    
                    if description_parts:
                        event.add('description', '\n\n'.join(description_parts))
                    
                    # Add to calendar
                    cal.add_component(event)
                    added_count += 1
                
                # Generate ICS file
                ics_data = cal.to_ical()
                b64 = base64.b64encode(ics_data).decode()
                href = f'<a href="data:text/calendar;base64,{b64}" download="{filename}.ics">Download iCalendar file ({added_count} events)</a>'
                st.markdown(href, unsafe_allow_html=True)
            
            # Export for integration with other golf tools
            if st.checkbox("Generate data for integration with other golf tools"):
                # Create custom JSON format
                integration_data = []
                
                for tournament in export_data:
                    # Create simplified structure
                    entry = {
                        'name': tournament.get('Tournament Name', ''),
                        'date': tournament.get('Date', ''),
                        'venue': tournament.get('Golf Course Name', ''),
                        'location': tournament.get('Location', ''),
                        'type': tournament.get('Tournament Type', ''),
                        'isQualifier': tournament.get('Is Qualifier', False),
                    }
                    
                    # Add optional fields if available
                    if tournament.get('Days'):
                        entry['days'] = tournament.get('Days')
                    
                    if tournament.get('Entry Fee'):
                        entry['entryFee'] = tournament.get('Entry Fee')
                    
                    if tournament.get('Prize Money'):
                        entry['prizeMoney'] = tournament.get('Prize Money')
                    
                    if tournament.get('Parent Tournament'):
                        entry['parentTournament'] = tournament.get('Parent Tournament')
                    
                    if tournament.get('Parent Tournament ID'):
                        entry['parentTournamentId'] = tournament.get('Parent Tournament ID')
                    
                    integration_data.append(entry)
                
                # Generate integration JSON
                json_str = json.dumps(integration_data, indent=2)
                b64 = base64.b64encode(json_str.encode()).decode()
                href = f'<a href="data:application/json;base64,{b64}" download="{filename}_integration.json">Download Integration JSON</a>'
                st.markdown(href, unsafe_allow_html=True)
    
    with tab4:
        st.subheader("Site Profiles")
        
        # Display available site profiles
        profiles = TournamentSiteDetector.get_all_profiles()
        
        st.write("""
        The scraper can use optimized profiles for specific golf tournament websites.
        These profiles help improve the data extraction quality for supported sites.
        """)
        
        # Show supported sites
        st.write("**Currently supported sites:**")
        for profile_id, profile in profiles.items():
            st.write(f"- **{profile['name']}** ({profile_id})")
        
        # Add new profile section
        st.subheader("Add Custom Site Profile")
        
        with st.expander("Define a new site profile"):
            new_profile_name = st.text_input("Site Name", value="")
            new_profile_id = st.text_input("Profile ID", value="").lower()
            
            # Pattern matching
            st.write("Add URL patterns to match (comma separated):")
            patterns = st.text_input("Patterns", value="")
            
            # Selector configuration
            st.write("Configure selectors (advanced):")
            
            col1, col2 = st.columns(2)
            
            with col1:
                tournament_selector = st.text_input("Tournament selector", value="")
                name_selector = st.text_input("Name selector", value="")
            
            with col2:
                date_selector = st.text_input("Date selector", value="")
                location_selector = st.text_input("Location selector", value="")
            
            if st.button("Add Profile"):
                if new_profile_name and new_profile_id and tournament_selector:
                    # Create new profile structure
                    new_profile = {
                        'name': new_profile_name,
                        'patterns': [p.strip() for p in patterns.split(',') if p.strip()],
                        'selectors': {
                            'tournaments': [
                                {'selector': tournament_selector, 'type': 'auto'}
                            ],
                            'name': [
                                {'selector': name_selector or 'a', 'attribute': 'text'}
                            ],
                            'date': [
                                {'selector': date_selector or 'span.date', 'attribute': 'text'}
                            ],
                            'location': [
                                {'selector': location_selector or 'span.location', 'attribute': 'text'}
                            ]
                        }
                    }
                    
                    # Store in session state
                    if 'custom_profiles' not in st.session_state:
                        st.session_state.custom_profiles = {}
                    
                    st.session_state.custom_profiles[new_profile_id] = new_profile
                    
                    st.success(f"Added profile: {new_profile_name}")
                else:
                    st.error("Please provide at least a name, ID, and tournament selector")
        
        # Show custom profiles
        if 'custom_profiles' in st.session_state and st.session_state.custom_profiles:
            st.subheader("Your Custom Profiles")
            
            for profile_id, profile in st.session_state.custom_profiles.items():
                st.write(f"- **{profile['name']}** ({profile_id})")
    
    # Add helpful instructions
    with st.expander("Tips for Better Results"):
        st.markdown("""
        ### Getting the Best Results
        
        - **URL Selection**: Use URLs that point directly to tournament listing pages
        - **Site Profiles**: For supported sites, using the optimized site profile will improve results
        - **Detail Pages**: The scraper will follow links to detail pages to extract more information
        - **Qualifier Detection**: The tool looks for qualifying rounds mentioned on tournament pages
        - **Performance**: Use multi-threading for faster results with many detail pages
        - **Caching**: Results are cached to improve performance on repeated scrapes
        - **Exports**: Use the Export tab to generate files for different use cases
        - **Visualization**: The Visualizations tab provides insights about tournament patterns
        
        ### Troubleshooting
        
        - If no tournaments are found, try adjusting the URL to point to a listing page
        - Enable "Show debug information" in Advanced Options to see more details
        - For partially extracted data, you can manually complete missing fields before export
        - Try different site profiles if the default scraping doesn't work well
        """)

if __name__ == "__main__":
    main()
