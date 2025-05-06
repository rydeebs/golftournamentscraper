# START OF FILE app (1).py

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

# --- Helper Functions (CSV/JSON Download, Cache, HTTP Get) ---
# (Keep existing get_table_download_link, get_json_download_link,
#  save_to_cache, load_from_cache, get_page_html functions as they are)

# Function to download data as CSV
def get_table_download_link(data, filename="tournament_data.csv"):
    """Generates a link allowing the data to be downloaded as CSV"""
    if not data:
        return ""
    # Ensure data is a list of dicts for pandas
    if not isinstance(data, list) or (data and not isinstance(data[0], dict)):
         logger.error("Data for CSV export is not a list of dictionaries.")
         # Try to convert if it's a DataFrame
         if isinstance(data, pd.DataFrame):
             data = data.to_dict('records')
         else:
             return "<p>Error: Invalid data format for CSV export.</p>"

    # Use pandas for robust CSV writing
    try:
        df = pd.DataFrame(data)
        # Reorder columns to match desired output if possible
        desired_cols = ['Original Date', 'Name', 'Course', 'City', 'State', 'Zip']
        # Get actual columns present in the dataframe
        actual_cols = df.columns.tolist()
        # Create final column order: desired cols first, then any others
        final_cols = [col for col in desired_cols if col in actual_cols]
        other_cols = [col for col in actual_cols if col not in final_cols]
        df = df[final_cols + other_cols]

        csv_string = df.to_csv(index=False)
        b64 = base64.b64encode(csv_string.encode()).decode()
        href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">Download CSV file</a>'
        return href
    except Exception as e:
        logger.error(f"Error generating CSV link: {e}")
        return f"<p>Error generating CSV: {e}</p>"


# Function to download data as JSON
def get_json_download_link(data, filename="tournament_data.json"):
    """Generates a link allowing the data to be downloaded as JSON"""
    if not data:
        return ""
    try:
        json_string = json.dumps(data, indent=2)
        b64 = base64.b64encode(json_string.encode()).decode()
        href = f'<a href="data:file/json;base64,{b64}" download="{filename}">Download JSON file</a>'
        return href
    except Exception as e:
        logger.error(f"Error generating JSON link: {e}")
        return f"<p>Error generating JSON: {e}</p>"

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
            # Ensure URL has scheme
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url # Default to https
            response = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            response.raise_for_status()
            # Try to decode using apparent encoding, fallback to utf-8
            try:
                html_content = response.content.decode(response.apparent_encoding)
            except (UnicodeDecodeError, LookupError):
                html_content = response.content.decode('utf-8', errors='replace')
            return html_content
        except requests.RequestException as e:
            retry_count += 1
            error_message = f"Error fetching {url}: {str(e)}"
            logger.warning(f"Attempt {retry_count} failed: {error_message}")
            if retry_count >= max_retries:
                st.error(f"Failed to fetch data from {url} after {max_retries} attempts. Error: {str(e)}")
                return None
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
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2) # Use indent for readability
    except Exception as e:
        logger.error(f"Failed to save cache file {cache_file}: {e}")


# Function to load data from cache
def load_from_cache(key, max_age_hours=24):
    """Load data from cache if not expired"""
    cache_dir = ".cache"
    cache_file = os.path.join(cache_dir, f"{key}.json")

    # Check if cache file exists
    if not os.path.exists(cache_file):
        return None

    # Check if cache file is expired
    try:
        file_age = time.time() - os.path.getmtime(cache_file)
        if file_age > (max_age_hours * 3600):
            logger.info(f"Cache expired for key: {key}")
            return None
    except OSError as e:
        logger.error(f"Error checking cache file age {cache_file}: {e}")
        return None

    # Load data from cache file
    try:
        with open(cache_file, 'r', encoding='utf-8') as f:
            logger.info(f"Loading data from cache for key: {key}")
            return json.load(f)
    except (json.JSONDecodeError, IOError, UnicodeDecodeError) as e:
        logger.error(f"Error loading cache file {cache_file}: {e}")
        # Attempt to delete corrupted cache file
        try:
            os.remove(cache_file)
            logger.info(f"Removed potentially corrupted cache file: {cache_file}")
        except OSError as delete_error:
            logger.error(f"Failed to remove corrupted cache file {cache_file}: {delete_error}")
        return None

# --- Parsing and Extraction Functions ---

# Initialize a tournament data dictionary with desired fields
def initialize_tournament_data():
    return {
        'Original Date': None, # Store the raw date string found
        'Name': None,
        'Course': None,
        'City': None,
        'State': None,
        'Zip': None,
        'Date': None, # Parsed date (YYYY-MM-DD or TBD)
        'Detail URL': None,
        'Tournament ID': None,
        'Is Qualifier': False,
        'Has Qualifiers': None,
        'Qualifier Count': None,
        'Parent Tournament': None,
        'Parent Tournament ID': None,
        'Eligibility': None,
        'Description': None,
        'Tournament Type': None,
        # Add other fields from original script if needed later
    }

# Function to parse date string into a standardized format
def parse_date(date_string):
    """Parse various date formats into YYYY-MM-DD or returns 'TBD' or original string."""
    if not date_string:
        return None

    date_string = str(date_string).strip() # Ensure it's a string

    # Handle empty or invalid strings
    if date_string.lower() in ['tbd', 'tba', 'to be determined', 'to be announced', '']:
        return 'TBD'

    # Handle common date formats (prioritize formats closer to screenshot)
    date_formats = [
        '%m/%d/%Y',        # 5/28/2025
        '%m/%d/%y',        # 5/28/25 (will assume current century or previous if < 70)
        '%B %d, %Y',       # May 19, 2025
        '%b %d, %Y',       # Jan 1, 2023
        '%Y-%m-%d',        # 2023-01-01
        '%m-%d-%Y',        # 01-01-2023
        '%d %B %Y',        # 1 January 2023
        '%d %b %Y',        # 1 Jan 2023
        # Add formats for ranges if needed, extracting start date
        '%B %d-%d, %Y',    # January 1-2, 2023 -> extract Jan 1, 2023
        '%b %d-%d, %Y',    # Jan 1-2, 2023 -> extract Jan 1, 2023
        '%m/%d-%m/%d/%Y',  # 05/19-05/20/2025 -> extract 05/19/2025
        '%m/%d/%Y - %m/%d/%Y', # 05/19/2025 - 05/20/2025 -> extract 05/19/2025
    ]

    for date_format in date_formats:
        try:
            # Handle ranges - extract the start date part
            if '%' in date_format and '-' in date_format and date_format.count('%') > 3: # Simple range detection
                 # Attempt to parse only the start part (crude but might work)
                 parts = date_string.split('-')
                 if parts:
                     parsed_date = datetime.strptime(parts[0].strip(), date_format.split('-')[0].strip())
                 else:
                     continue # Skip if split fails
            elif date_format == '%m/%d-%m/%d/%Y':
                 parts = date_string.split('-')
                 year_part = parts[1].split('/')[-1]
                 start_part = parts[0] + '/' + year_part
                 parsed_date = datetime.strptime(start_part, '%m/%d/%Y')
            elif date_format == '%m/%d/%Y - %m/%d/%Y':
                 parts = date_string.split(' - ')
                 parsed_date = datetime.strptime(parts[0].strip(), '%m/%d/%Y')
            else:
                 parsed_date = datetime.strptime(date_string, date_format)

            # Handle two-digit year
            if parsed_date.year < 100:
                current_year = datetime.now().year
                century = (current_year // 100) * 100
                parsed_year = century + parsed_date.year
                # If the resulting year is far in the future, assume previous century
                if parsed_year > current_year + 10:
                     parsed_year -= 100
                parsed_date = parsed_date.replace(year=parsed_year)


            return parsed_date.strftime('%Y-%m-%d')  # Return in ISO format
        except (ValueError, IndexError):
            continue

    # Handle date ranges by taking the start date (more robustly)
    # Pattern: Month Day(st/nd/rd/th) [-/–] (Month)? Day(st/nd/rd/th)?, Year
    date_range_match = re.search(
        r'(\w+\.?\s+\d{1,2}(?:st|nd|rd|th)?)\s*[-–]\s*(?:\w+\.?\s+)?(\d{1,2}(?:st|nd|rd|th)?),?\s*(\d{4})',
        date_string, re.I
    )
    if date_range_match:
        start_date_str, _, year_str = date_range_match.groups()
        full_start_str = f"{start_date_str.replace('.','').strip()}, {year_str}"
        # Try parsing with different month formats
        for fmt in ['%B %d, %Y', '%b %d, %Y']:
             try:
                 parsed_date = datetime.strptime(full_start_str, fmt)
                 return parsed_date.strftime('%Y-%m-%d')
             except ValueError:
                 continue

    # Try extracting just month, day, year components if other formats failed
    # Relaxed pattern to find components anywhere
    month_pattern = r'\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\b\.?'
    day_pattern = r'\b(\d{1,2})(?:st|nd|rd|th)?\b'
    year_pattern = r'\b(\d{4})\b'

    month_match = re.search(month_pattern, date_string, re.I)
    day_match = re.search(day_pattern, date_string)
    year_match = re.search(year_pattern, date_string)

    if month_match and day_match and year_match:
        month_str = month_match.group(1)
        day_str = day_match.group(1)
        year_str = year_match.group(1)
        try:
            date_str_constructed = f"{month_str} {day_str}, {year_str}"
            # Try full month name first, then abbreviated
            try:
                parsed_date = datetime.strptime(date_str_constructed, '%B %d, %Y')
            except ValueError:
                parsed_date = datetime.strptime(date_str_constructed, '%b %d, %Y')
            return parsed_date.strftime('%Y-%m-%d')
        except ValueError:
            pass # Fall through if construction fails

    logger.warning(f"Could not parse date: {date_string}. Returning original.")
    return date_string # Return original if no format matches


# Function to extract location (City, State, Zip) from a text string
def extract_location(text):
    """Extract city, state, and zip from location text. Returns a dict."""
    location_info = {'city': None, 'state': None, 'zip': None}
    if not text:
        return location_info

    text = str(text).strip()

    # Pattern 1: City, ST ZIP (ZIP optional, 5 or 5-4) - Prioritize this common format
    # Handles potential extra spaces or periods in city names
    match = re.search(r'([A-Za-z\s\.-]+?),\s*([A-Z]{2})\s*(\d{5}(?:-\d{4})?)?', text)
    if match:
        location_info['city'] = match.group(1).strip().rstrip(',.')
        location_info['state'] = match.group(2).strip()
        location_info['zip'] = match.group(3).strip() if match.group(3) else None
        return location_info

    # Pattern 2: City, StateName ZIP (ZIP optional)
    match = re.search(r'([A-Za-z\s\.-]+?),\s*([A-Za-z]{3,})\s*(\d{5}(?:-\d{4})?)?', text)
    if match:
        # Check if the "StateName" might actually be a state abbreviation mistakenly matched
        potential_state_name = match.group(2).strip()
        if len(potential_state_name) > 2: # Reasonably sure it's a full name
             location_info['city'] = match.group(1).strip().rstrip(',.')
             location_info['state'] = potential_state_name # Keep full name for now
             location_info['zip'] = match.group(3).strip() if match.group(3) else None
             # Optionally map full state name to abbreviation here if needed
             return location_info
        # If it was short, it might have been caught by Pattern 1 ideally, but check again
        elif len(potential_state_name) == 2 and potential_state_name.isupper():
             location_info['city'] = match.group(1).strip().rstrip(',.')
             location_info['state'] = potential_state_name # Assume it's an abbreviation
             location_info['zip'] = match.group(3).strip() if match.group(3) else None
             return location_info


    # Pattern 3: Just City, ST (no zip)
    match = re.search(r'([A-Za-z\s\.-]+?),\s*([A-Z]{2})\b', text)
    if match:
        location_info['city'] = match.group(1).strip().rstrip(',.')
        location_info['state'] = match.group(2).strip()
        return location_info

    # Pattern 4: Just City, StateName (no zip)
    match = re.search(r'([A-Za-z\s\.-]+?),\s*([A-Za-z]{3,})\b', text)
    if match:
         potential_state_name = match.group(2).strip()
         if len(potential_state_name) > 2:
             location_info['city'] = match.group(1).strip().rstrip(',.')
             location_info['state'] = potential_state_name
             return location_info
         elif len(potential_state_name) == 2 and potential_state_name.isupper():
              location_info['city'] = match.group(1).strip().rstrip(',.')
              location_info['state'] = potential_state_name
              return location_info

    # Fallback: If no pattern matches, return the original text perhaps as city?
    # Or maybe leave all as None if format is unexpected. Let's leave as None.
    # logger.warning(f"Could not extract structured location from: {text}")
    return location_info


# Function to extract golf course name
def extract_golf_course(text):
    """Extract golf course name from text"""
    if not text:
        return None

    text = str(text).strip()

    # Common patterns for golf course names - make more specific
    # Look for keywords preceded by potential name characters
    # Prioritize longer matches and those starting with "at" or "venue"
    patterns = [
        r'(?:at|venue|course|host site)\s*[:\-]?\s*([\w\s.&\'\-]+(?:Golf\s*Club|Country\s*Club|Golf\s*Course|G\.?C\.?C?\.?|Golf|Links|Course|Club|Plantation|Preserve|National|Resort|Park)\b)',
        r'\b([\w\s.&\'\-]+(?:Golf\s*Club|Country\s*Club|Golf\s*Course|G\.?C\.?C?\.?|Golf|Links|Course|Club|Plantation|Preserve|National|Resort|Park))\b'
    ]

    best_match = None
    for pattern in patterns:
        matches = re.finditer(pattern, text, re.I)
        for match in matches:
            candidate = match.group(1).strip()
            # Simple filter: avoid matching just city/state
            if ',' not in candidate and len(candidate) > 3:
                # Prefer longer matches if multiple found
                if best_match is None or len(candidate) > len(best_match):
                    best_match = candidate

    if best_match:
        # Clean up trailing punctuation if any
        best_match = re.sub(r'[\s,.-]+$', '', best_match)
        return best_match

    # Fallback if no pattern matches - return None
    return None

# Function to generate a unique ID for a tournament
def generate_tournament_id(tournament_data):
    """Generate a unique ID for a tournament based on its data"""
    # Use name and original date string as key components
    key_data = f"{tournament_data.get('Name', '')}-{tournament_data.get('Original Date', '')}"

    # Add location if available
    if tournament_data.get('City') and tournament_data.get('State'):
        key_data += f"-{tournament_data.get('City')}-{tournament_data.get('State')}"
    elif tournament_data.get('Location'): # Fallback to combined location
         key_data += f"-{tournament_data.get('Location')}"


    # Add course if available
    if tournament_data.get('Course'):
        key_data += f"-{tournament_data.get('Course')}"

    # Generate hash
    return hashlib.md5(key_data.encode()).hexdigest()[:12] # Slightly longer ID

# Function to determine if a tournament is a qualifier based on name
def is_qualifier_tournament(name):
    """Determine if a tournament is a qualifier based on name"""
    if not name:
        return False

    name_lower = str(name).lower()
    qualifier_keywords = [
        'qualifier', 'qualifying', 'qualification', 'q-school', 'q school',
        'local qualifying', 'sectional qualifying', 'regional qualifying'
    ]

    return any(keyword in name_lower for keyword in qualifier_keywords)


# --- Site Specific and Generic Parsers ---

# Function to handle site type detection
def detect_site_type(url, html=None):
    """Detect the type of golf site from URL and HTML content"""
    if not url: return 'generic'
    url_lower = url.lower()
    # Check for common golf site patterns in URL
    if 'fsga.org' in url_lower:
        return 'fsga'
    elif 'golfgenius.com' in url_lower: # More specific check
        return 'golfgenius'
    elif 'bluegolf.com' in url_lower: # More specific check
        return 'bluegolf'
    elif 'amateurgolf.com' in url_lower:
         return 'amateurgolf' # Example site type

    # If HTML is provided, check content for site indicators
    if html:
        # Use regex for broader matching within HTML content
        html_lower = html.lower()
        if re.search(r'florida state golf association|fsga', html_lower):
            return 'fsga'
        if re.search(r'golfgenius|golf genius', html_lower):
            return 'golfgenius'
        # BlueGolf often includes 'bluegolf' in CSS classes, script URLs, or footer links
        if re.search(r'bluegolf|blue golf|\'bluegolf\.com\'', html_lower):
            return 'bluegolf'
        if re.search(r'amateurgolf\.com', html_lower):
             return 'amateurgolf'


    # Default to generic
    return 'generic'

# **REVISED** Generic Parser focusing on table structure for schedule pages
def parse_generic_tournament_item(element, base_url, site_type='generic'):
    """Parse a generic tournament list item, prioritizing table rows (tr)."""
    tournament_data = initialize_tournament_data()

    # --- Handle Table Rows (tr) ---
    if element.name == 'tr':
        cells = element.find_all(['td', 'th']) # Find all cells in the row
        if len(cells) < 3: # Need at least Date, Name, Course/Location usually
            return None # Skip rows that don't seem to have enough data

        # Attempt to map cell content to fields - This is heuristic!
        possible_date = None
        possible_name = None
        possible_course = None
        possible_location_text = None

        # Iterate through cells and guess content type
        potential_fields = {}
        for i, cell in enumerate(cells):
            cell_text = cell.get_text(separator=' ', strip=True)
            if not cell_text:
                continue

            # Check for Date (using regex for common patterns)
            if re.search(r'\d{1,2}/\d{1,2}/\d{2,4}|\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b', cell_text, re.I):
                 if 'Date' not in potential_fields: potential_fields['Date'] = (cell_text, i)
                 continue # Move to next cell

            # Check for Course Name
            if re.search(r'Club|Course|Links|National|Plantation|Preserve|G\.?C\.?C?\.?|Country|Golf', cell_text, re.I):
                 if 'Course' not in potential_fields: potential_fields['Course'] = (cell_text, i)
                 # continue # Course might also contain location info

            # Check for Location (City, ST format)
            if re.search(r',\s*[A-Z]{2}\b', cell_text):
                 if 'Location' not in potential_fields: potential_fields['Location'] = (cell_text, i)
                 continue # Move to next cell

            # Check for Name (often longer text, might be a link)
            link = cell.find('a')
            cell_len = len(cell_text)
            # Heuristic: Name is often longer than date/location, might contain keywords
            if cell_len > 10 and any(kw in cell_text.lower() for kw in ['champ', 'open', 'invit', 'classic', 'amateur', 'tour', 'event']):
                if 'Name' not in potential_fields: potential_fields['Name'] = (cell_text, i)
                if link and 'href' in link.attrs:
                   potential_fields['Link'] = (link['href'], i)
                continue
            elif link and cell_len > 5: # Fallback: Assume a link with some text is the name
                if 'Name' not in potential_fields: potential_fields['Name'] = (cell_text, i)
                if 'href' in link.attrs:
                    potential_fields['Link'] = (link['href'], i)
                continue


        # --- Assign based on guesses ---
        if 'Date' in potential_fields: tournament_data['Original Date'] = potential_fields['Date'][0]
        if 'Name' in potential_fields: tournament_data['Name'] = potential_fields['Name'][0]
        if 'Course' in potential_fields: tournament_data['Course'] = extract_golf_course(potential_fields['Course'][0]) # Refine course name
        if 'Location' in potential_fields:
             loc_text = potential_fields['Location'][0]
             # If course name wasn't found separately, try extracting it from location cell
             if not tournament_data['Course']:
                  tournament_data['Course'] = extract_golf_course(loc_text)
             # Extract City/State/Zip
             location_info = extract_location(loc_text)
             tournament_data['City'] = location_info['city']
             tournament_data['State'] = location_info['state']
             tournament_data['Zip'] = location_info['zip']
        if 'Link' in potential_fields:
             url = potential_fields['Link'][0]
             if not url.startswith(('http://', 'https://', '#', 'javascript:')):
                 try:
                     tournament_data['Detail URL'] = urljoin(base_url, url)
                 except ValueError:
                     logger.warning(f"Could not join base_url '{base_url}' with link '{url}'")
                     tournament_data['Detail URL'] = url # Store as is
             elif url.startswith(('http://', 'https://')):
                  tournament_data['Detail URL'] = url


        # Fallback: If heuristic mapping failed, try fixed indices (less reliable)
        # Example: Assumes Date=Col0, Name=Col1, Course=Col2, Location=Col3
        if not tournament_data['Original Date'] and len(cells) > 0: tournament_data['Original Date'] = cells[0].get_text(strip=True)
        if not tournament_data['Name'] and len(cells) > 1:
            name_cell = cells[1]
            tournament_data['Name'] = name_cell.get_text(strip=True)
            link = name_cell.find('a')
            if link and 'href' in link.attrs:
                url = link['href']
                if not url.startswith(('http://', 'https://', '#', 'javascript:')):
                    try:
                       tournament_data['Detail URL'] = urljoin(base_url, url)
                    except ValueError:
                        logger.warning(f"Could not join base_url '{base_url}' with link '{url}'")
                        tournament_data['Detail URL'] = url
                elif url.startswith(('http://', 'https://')):
                    tournament_data['Detail URL'] = url

        if not tournament_data['Course'] and len(cells) > 2: tournament_data['Course'] = extract_golf_course(cells[2].get_text(strip=True))
        if not tournament_data['City'] and len(cells) > 3:
            location_info = extract_location(cells[3].get_text(strip=True))
            tournament_data['City'] = location_info['city']
            tournament_data['State'] = location_info['state']
            tournament_data['Zip'] = location_info['zip']
             # If course name is still missing, try the location cell
            if not tournament_data['Course']:
                 tournament_data['Course'] = extract_golf_course(cells[3].get_text(strip=True))

    # --- Handle Non-Table Row Elements (div, li, etc.) - simplified ---
    else:
        # Try to find distinct elements for each piece of data
        name_text = element.get_text(strip=True) # Default name
        detail_url = None
        link = element.find('a')
        if link:
            name_text = link.get_text(strip=True) or name_text # Prefer link text
            if 'href' in link.attrs:
                url = link['href']
                if not url.startswith(('http://', 'https://', '#', 'javascript:')):
                    try:
                         detail_url = urljoin(base_url, url)
                    except ValueError:
                         logger.warning(f"Could not join base_url '{base_url}' with link '{url}'")
                         detail_url = url
                elif url.startswith(('http://', 'https://')):
                     detail_url = url

        if not name_text or len(name_text) < 5: return None # Basic filter

        tournament_data['Name'] = name_text
        tournament_data['Detail URL'] = detail_url

        # Find Date, Course, Location within the element's text
        element_text = element.get_text(separator=' ', strip=True)

        # Date
        date_match = re.search(r'\b(\d{1,2}/\d{1,2}/\d{2,4})\b|\b((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:st|nd|rd|th)?(?:,\s*\d{4})?)\b', element_text, re.I)
        if date_match:
            tournament_data['Original Date'] = date_match.group(0).strip()

        # Course
        tournament_data['Course'] = extract_golf_course(element_text)

        # Location
        location_info = extract_location(element_text)
        tournament_data['City'] = location_info['city']
        tournament_data['State'] = location_info['state']
        tournament_data['Zip'] = location_info['zip']


    # --- Post Processing ---
    # Basic validation: Need at least a Name and Date
    if not tournament_data['Name'] or not tournament_data['Original Date']:
        # logger.debug(f"Skipping item, missing Name or Date: {tournament_data}")
        return None

    # Parse the date
    tournament_data['Date'] = parse_date(tournament_data['Original Date'])

    # Determine Type/Qualifier Status
    tournament_data['Is Qualifier'] = is_qualifier_tournament(tournament_data['Name'])
    if tournament_data['Is Qualifier']:
        tournament_data['Tournament Type'] = 'Qualifying Round'
    # Add more type logic here if needed (Amateur, Junior, etc.) based on Name

    # Generate ID
    tournament_data['Tournament ID'] = generate_tournament_id(tournament_data)

    # logger.debug(f"Parsed Item: {tournament_data}")
    return tournament_data

# GolfGenius Schedule Parser (Example Refinement)
def extract_golfgenius_schedule_tournaments(soup, url, max_details=None, show_progress=True, progress_bar=None, progress_text=None):
    """Extract tournaments from a GolfGenius schedule page, focusing on tables."""
    tournaments = []
    processed_ids = set()

    # Prioritize finding tables that look like schedules
    schedule_tables = soup.find_all('table', class_=lambda c: c and ('table' in c or 'schedule' in c or 'events' in c))
    if not schedule_tables:
        schedule_tables = soup.find_all('table') # Broader search

    tournament_elements = []
    for table in schedule_tables:
        rows = table.find_all('tr')
        # Skip header row if present (th elements)
        data_rows = [row for row in rows if row.find('td')] # Only rows with data cells
        if len(data_rows) > 1: # Check if table seems to contain data
             tournament_elements.extend(data_rows)
             logger.info(f"Found {len(data_rows)} potential tournament rows in a table.")


    # Fallback: If no suitable table found, look for list items or divs
    if not tournament_elements:
        logger.info("No suitable table found, looking for list/div elements.")
        selectors = [
            '.event-list-item', '.list-group-item', '.event-block', '.card', 'div.row'
        ]
        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                tournament_elements.extend(elements)
                logger.info(f"Found {len(elements)} potential elements using selector '{selector}'")


    # Fallback: Look for event links directly if other methods fail
    if not tournament_elements:
         logger.info("No list/div elements found, looking for direct event links.")
         # Links containing event/tournament/etc. in href or text
         event_links = soup.find_all('a', href=lambda h: h and re.search(r'event|tourn|pages/\d+', h, re.I))
         if event_links:
              logger.info(f"Found {len(event_links)} potential direct links.")
              # Try to get parent container for context
              for link in event_links:
                   parent = link.find_parent(['li', 'div', 'tr'])
                   tournament_elements.append(parent if parent else link)


    # Get total count for progress calculation
    total_elements = len(tournament_elements)
    logger.info(f"Total potential tournament elements found: {total_elements}")


    # Process each tournament element
    for i, element in enumerate(tournament_elements):
        # Update progress
        if show_progress and progress_bar and total_elements > 0:
            progress = (i + 1) / total_elements
            progress_bar.progress(progress)
            if progress_text:
                progress_text.text(f"Processing GolfGenius item {i+1} of {total_elements}...")

        # Parse the element
        tournament = parse_generic_tournament_item(element, url, site_type='golfgenius')

        if tournament and tournament['Tournament ID'] not in processed_ids:
            tournaments.append(tournament)
            processed_ids.add(tournament['Tournament ID'])

            # *Optional: Scrape detail page for more info (like Zip) if needed and enabled*
            # if tournament.get('Detail URL') and (max_details is None or len(processed_ids) <= max_details):
            #     # ... (logic to fetch detail page and update tournament data) ...
            #     pass


    logger.info(f"Extracted {len(tournaments)} unique tournaments from GolfGenius schedule.")
    return tournaments

# BlueGolf Schedule Parser (Example Refinement)
def extract_bluegolf_schedule_tournaments(soup, url, max_details=None, show_progress=True, progress_bar=None, progress_text=None):
    """Extract tournaments from a BlueGolf schedule page, focusing on tables."""
    tournaments = []
    processed_ids = set()

    # BlueGolf often uses tables with specific classes or structures
    schedule_tables = soup.find_all('table', class_=re.compile(r'schedule|events|tournaments', re.I))
    if not schedule_tables:
        schedule_tables = soup.find_all('table', id=re.compile(r'schedule|events', re.I))
    if not schedule_tables:
        schedule_tables = soup.find_all('table') # Fallback

    tournament_elements = []
    for table in schedule_tables:
        # Look for rows with a link likely pointing to a tournament/event
        rows = table.find_all('tr', {'onclick': True}) # Rows with onclick might link to details
        if not rows:
             rows = table.find_all('tr')

        data_rows = [row for row in rows if row.find('td')] # Only rows with data cells
        if len(data_rows) > 1:
             tournament_elements.extend(data_rows)
             logger.info(f"Found {len(data_rows)} potential tournament rows in a BlueGolf table.")


    # Fallback if no tables found
    if not tournament_elements:
         logger.info("No suitable table found on BlueGolf page, looking for list/div elements.")
         # Common BlueGolf list item classes
         selectors = ['tr.tournamentItem', 'div.listItem', '.eventItem', '.scheduleItem']
         for selector in selectors:
            elements = soup.select(selector)
            if elements:
                tournament_elements.extend(elements)
                logger.info(f"Found {len(elements)} potential elements using selector '{selector}'")


    total_elements = len(tournament_elements)
    logger.info(f"Total potential BlueGolf tournament elements found: {total_elements}")

    # Process each tournament element
    for i, element in enumerate(tournament_elements):
        # Update progress
        if show_progress and progress_bar and total_elements > 0:
            progress = (i + 1) / total_elements
            progress_bar.progress(progress)
            if progress_text:
                progress_text.text(f"Processing BlueGolf item {i+1} of {total_elements}...")

        # Parse the element
        tournament = parse_generic_tournament_item(element, url, site_type='bluegolf')

        if tournament and tournament['Tournament ID'] not in processed_ids:
            tournaments.append(tournament)
            processed_ids.add(tournament['Tournament ID'])
            # Optional: Detail page scraping


    logger.info(f"Extracted {len(tournaments)} unique tournaments from BlueGolf schedule.")
    return tournaments


# FSGA Schedule Parser (Refined)
def extract_fsga_schedule_tournaments(soup, url, max_details=None, show_progress=True, progress_bar=None, progress_text=None):
    """Extract tournaments from an FSGA schedule page (often lists or simple tables)."""
    tournaments = []
    processed_ids = set()

    # FSGA schedule pages might be simpler lists or divs rather than complex tables
    # Look for common container patterns
    tournament_elements = []
    selectors = [
        'div.tournament-list-item', # Check specific FSGA classes first
        'div.event-list-item',
        'div.card.tournament-card',
        'li.event-item',
        'table.dataTable tbody tr', # DataTables are sometimes used
        'table tr' # Generic table rows as fallback
    ]

    for selector in selectors:
        elements = soup.select(selector)
        if elements:
            tournament_elements.extend(elements)
            logger.info(f"Found {len(elements)} potential FSGA elements using selector '{selector}'")
            break # Stop if a specific selector works

    # If still none, maybe it's a very simple structure (e.g., just divs with info)
    if not tournament_elements:
         # Try finding divs that contain a date and a link
         potential_divs = soup.find_all('div')
         for div in potential_divs:
              has_date = re.search(r'\d{1,2}/\d{1,2}/\d{2,4}|\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b', div.get_text(), re.I)
              has_link = div.find('a', href=True)
              if has_date and has_link:
                   tournament_elements.append(div)
         if tournament_elements:
              logger.info(f"Found {len(tournament_elements)} potential FSGA divs containing date and link.")

    total_elements = len(tournament_elements)
    logger.info(f"Total potential FSGA tournament elements found: {total_elements}")

    # Process each tournament element
    for i, element in enumerate(tournament_elements):
        # Update progress
        if show_progress and progress_bar and total_elements > 0:
            progress = (i + 1) / total_elements
            progress_bar.progress(progress)
            if progress_text:
                progress_text.text(f"Processing FSGA item {i+1} of {total_elements}...")

        # Parse the element using the generic parser (or a dedicated FSGA one if needed)
        tournament = parse_generic_tournament_item(element, url, site_type='fsga')

        if tournament and tournament['Tournament ID'] not in processed_ids:
            # Specific FSGA refinement: Try finding location/course in dedicated elements if missing
            if not tournament['City']:
                loc_element = element.select_one('.location, .tournament-location')
                if loc_element:
                    location_info = extract_location(loc_element.get_text(strip=True))
                    tournament['City'] = location_info['city']
                    tournament['State'] = location_info['state']
                    tournament['Zip'] = location_info['zip']
            if not tournament['Course']:
                 course_element = element.select_one('.course, .venue')
                 if course_element:
                      tournament['Course'] = extract_golf_course(course_element.get_text(strip=True))

            tournaments.append(tournament)
            processed_ids.add(tournament['Tournament ID'])
            # Optional: Detail page scraping

    logger.info(f"Extracted {len(tournaments)} unique tournaments from FSGA schedule.")
    return tournaments

# Generic Schedule Parser (Fallback)
def extract_generic_schedule_tournaments(soup, url, max_details=None, show_progress=True, progress_bar=None, progress_text=None):
    """Extract tournaments from a generic schedule page, prioritizing tables."""
    tournaments = []
    processed_ids = set()

    # Prioritize tables
    schedule_tables = soup.find_all('table')
    tournament_elements = []
    for table in schedule_tables:
        rows = table.find_all('tr')
        data_rows = [row for row in rows if row.find('td')]
        if len(data_rows) > 1:
             tournament_elements.extend(data_rows)
             logger.info(f"Found {len(data_rows)} potential tournament rows in a generic table.")

    # Fallback: Look for list items or divs
    if not tournament_elements:
        logger.info("No suitable table found, looking for generic list/div elements.")
        selectors = [
            'li', 'div.event', 'div.item', 'div.row', 'article', 'div.card'
        ]
        for selector in selectors:
            elements = soup.select(selector)
             # Filter out elements that are too small or clearly not tournament items
            elements = [el for el in elements if len(el.get_text(strip=True)) > 20 and el.find('a')]
            if elements:
                tournament_elements.extend(elements)
                logger.info(f"Found {len(elements)} potential elements using selector '{selector}'")

    total_elements = len(tournament_elements)
    logger.info(f"Total potential generic tournament elements found: {total_elements}")

    # Process each tournament element
    for i, element in enumerate(tournament_elements):
        # Update progress
        if show_progress and progress_bar and total_elements > 0:
            progress = (i + 1) / total_elements
            progress_bar.progress(progress)
            if progress_text:
                progress_text.text(f"Processing generic item {i+1} of {total_elements}...")

        # Parse the element
        tournament = parse_generic_tournament_item(element, url, site_type='generic')

        if tournament and tournament['Tournament ID'] not in processed_ids:
            tournaments.append(tournament)
            processed_ids.add(tournament['Tournament ID'])
            # Optional: Detail page scraping

    logger.info(f"Extracted {len(tournaments)} unique tournaments from generic schedule.")
    return tournaments


# Main Scraper Function - Selects schedule or detail parsing
def scrape_tournaments(url, max_details=None, show_progress=True):
    """
    Scrapes a tournament page (either schedule or detail).
    Prioritizes extracting Date, Name, Course, City, State, Zip.
    """
    # Check cache first
    cache_key = f"schedule_{hashlib.md5(url.encode()).hexdigest()}"
    cached_data = load_from_cache(cache_key)
    if cached_data:
        if show_progress:
            st.success(f"Loaded {len(cached_data)} tournaments from cache.")
        # Ensure data matches current structure (add missing keys)
        for item in cached_data:
            defaults = initialize_tournament_data()
            for key in defaults:
                item.setdefault(key, defaults[key])
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
        progress_text.text("Analyzing page and finding tournaments...")

    # Detect site type
    site_type = detect_site_type(url, html)
    logger.info(f"Detected site type: {site_type} for URL: {url}")

    # Determine if it's likely a schedule page (multiple items) vs detail page
    # Heuristic: Count potential tournament items
    potential_items_count = 0
    table_rows = soup.select('table tr td') # Count data cells
    list_items = soup.select('li, div.event, div.item, div.card') # Count list-like items
    potential_items_count = len(table_rows) // 3 + len(list_items) # Rough estimate

    is_schedule_page = potential_items_count > 5 # Arbitrary threshold

    logger.info(f"Potential items found: {potential_items_count}. Is schedule page? {is_schedule_page}")

    # --- Call appropriate extractor ---
    if is_schedule_page:
        if site_type == 'fsga':
            tournaments = extract_fsga_schedule_tournaments(soup, url, max_details, show_progress, progress_bar, progress_text)
        elif site_type == 'golfgenius':
            tournaments = extract_golfgenius_schedule_tournaments(soup, url, max_details, show_progress, progress_bar, progress_text)
        elif site_type == 'bluegolf':
            tournaments = extract_bluegolf_schedule_tournaments(soup, url, max_details, show_progress, progress_bar, progress_text)
        else: # Generic schedule extraction
            tournaments = extract_generic_schedule_tournaments(soup, url, max_details, show_progress, progress_bar, progress_text)
    else:
        # Assume Detail Page (or very short schedule) - use generic item parser on body or main content
        logger.info("Parsing as potential detail page or short list.")
        main_content = soup.find('main') or soup.find('body') # Try to narrow down scope
        if main_content:
             # Try parsing the main content area as a single item
             tournament = parse_generic_tournament_item(main_content, url, site_type=site_type)
             if tournament:
                  tournaments.append(tournament)
             else:
                  # If parsing whole content failed, fall back to generic schedule on the content
                  tournaments = extract_generic_schedule_tournaments(soup, url, max_details, show_progress, progress_bar, progress_text)

        else:
             tournaments = [] # No main content found


    # Save to cache
    if tournaments:
        # Ensure data matches current structure before caching
        for item in tournaments:
            defaults = initialize_tournament_data()
            for key in defaults:
                item.setdefault(key, defaults[key])
        save_to_cache(cache_key, tournaments)

    # Complete progress
    if show_progress:
        progress_bar.progress(1.0)
        progress_text.text(f"Found {len(tournaments)} tournaments.")

    return tournaments


# --- Streamlit UI ---

def main():
    st.set_page_config(page_title="Golf Tournament Scraper", layout="wide")

    st.title("Golf Tournament Scraper")

    st.markdown("""
    Enter the URL of a golf tournament **schedule page** to extract tournament information.
    The tool will attempt to find the Date, Name, Course, City, State, and Zip Code for each event listed.
    """)

    # Sidebar for configuration
    st.sidebar.title("Configuration")

    # Input for URL
    default_urls = [
        "https://wpga-onlineregistration.golfgenius.com/pages/1264528",
        "https://www.fsga.org/TournamentCategory/EnterList/d99ad47f-2e7d-4ff4-8a32-c5b1eb315d28?year=2025&p=2",
        "https://usamtour.bluegolf.com/bluegolf/usamtour25/schedule/index.htm?start=250&neweventschedule=event.*",
        "https://www.golfgenius.com/pages/1264528", # Add example detail page
    ]
    url = st.sidebar.text_input(
        "Tournament Schedule URL",
        # value=default_urls[0] # Default to first example
        value="https://wpga-onlineregistration.golfgenius.com/pages/1264528"
    )
    st.sidebar.markdown("Example URLs:")
    for ex_url in default_urls:
        st.sidebar.code(ex_url, language=None)


    # Advanced options
    with st.sidebar.expander("Advanced Options"):
        max_details_scrape = st.checkbox("Scrape detail pages (slower, more data)", value=False)
        max_details_num = st.number_input("Max detail pages (if enabled)", min_value=1, value=10, step=1, disabled=not max_details_scrape)
        max_details = max_details_num if max_details_scrape else 0 # Set to 0 if checkbox is off

        clear_cache_button = st.button("Clear Cache")
        if clear_cache_button:
            cache_dir = ".cache"
            if os.path.exists(cache_dir):
                import shutil
                try:
                    shutil.rmtree(cache_dir)
                    st.success("Cache cleared!")
                    # Clear session state as well
                    if 'tournaments' in st.session_state:
                         del st.session_state.tournaments

                except Exception as e:
                    st.error(f"Error clearing cache: {e}")
            else:
                st.info("Cache directory not found.")


        show_debug = st.checkbox("Show debug information in console", value=False)

        if show_debug:
            logging.getLogger().setLevel(logging.DEBUG)
            logger.info("Debug logging enabled.")
        else:
            logging.getLogger().setLevel(logging.INFO)

    # Initialize session state if not exists
    if 'tournaments' not in st.session_state:
        st.session_state.tournaments = []

    # Create tabs for different functionality
    tab1, tab2 = st.tabs(["Scraper Results", "Export"])

    with tab1:
        # Button to start scraping
        if st.button("Scrape Tournaments", type="primary"):
            if url:
                with st.spinner('Scraping data... Please wait.'):
                    try:
                        # Use the main scraper function
                        tournaments_result = scrape_tournaments(url, max_details=max_details, show_progress=True)

                        if not tournaments_result:
                            st.warning("No tournament data found. The website structure might not be supported or the page may be empty.")
                            st.session_state.tournaments = []
                        else:
                            # Store the data in session state
                            st.session_state.tournaments = tournaments_result
                            st.success(f"Found {len(tournaments_result)} tournaments.")

                    except Exception as e:
                        st.error(f"An error occurred during scraping: {str(e)}")
                        logger.exception("Scraping failed:") # Log traceback
                        st.session_state.tournaments = [] # Clear results on error
            else:
                st.error("Please enter a valid URL")

        # Display results if available in session state
        if st.session_state.tournaments:
            st.subheader("Tournament Data")

            # Prepare data for display - select and order columns
            display_data = []
            for t in st.session_state.tournaments:
                display_row = {
                    'Date': t.get('Original Date', 'N/A'), # Show original date string
                    'Name': t.get('Name', 'N/A'),
                    'Course': t.get('Course', 'N/A'),
                    'City': t.get('City', 'N/A'),
                    'State': t.get('State', 'N/A'),
                    'Zip': t.get('Zip', 'N/A')
                }
                display_data.append(display_row)

            # Show data table using Pandas DataFrame for better handling
            df_display = pd.DataFrame(display_data)
            st.dataframe(df_display, use_container_width=True)

            # Optional: Show raw data dump
            with st.expander("Show Raw Extracted Data"):
                 st.json(st.session_state.tournaments, expanded=False)

        elif not url: # Prompt user if URL is empty
             st.info("Enter a URL in the sidebar and click 'Scrape Tournaments'.")


    with tab2:
        if 'tournaments' in st.session_state and st.session_state.tournaments:
            st.subheader("Export Options")

            # Select columns for export
            all_columns = list(initialize_tournament_data().keys())
            default_export_cols = ['Original Date', 'Name', 'Course', 'City', 'State', 'Zip', 'Date', 'Detail URL', 'Tournament ID']
            selected_columns = st.multiselect(
                "Select columns to export:",
                options=all_columns,
                default=[col for col in default_export_cols if col in all_columns] # Ensure defaults exist
            )


            # Export format selection
            export_format = st.radio(
                "Select export format",
                ["CSV", "JSON", "Excel"]
            )

            # Custom filename
            filename = st.text_input("Filename (without extension)", value="golf_tournament_data")

            # Filter data to include only selected columns
            export_data = []
            if selected_columns:
                for t in st.session_state.tournaments:
                     row = {col: t.get(col) for col in selected_columns}
                     export_data.append(row)
            else:
                 st.warning("Please select at least one column to export.")
                 export_data = None # Prevent export if no columns selected

            # Create export links
            if export_data:
                 if export_format == "CSV":
                     st.markdown(get_table_download_link(export_data, f"{filename}.csv"), unsafe_allow_html=True)
                 elif export_format == "JSON":
                     st.markdown(get_json_download_link(export_data, f"{filename}.json"), unsafe_allow_html=True)
                 elif export_format == "Excel":
                     try:
                         excel_buffer = io.BytesIO()
                         df = pd.DataFrame(export_data)
                         df.to_excel(excel_buffer, index=False, engine='openpyxl') # Specify engine
                         excel_data = excel_buffer.getvalue()
                         b64 = base64.b64encode(excel_data).decode()
                         href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{filename}.xlsx">Download Excel file</a>'
                         st.markdown(href, unsafe_allow_html=True)
                     except Exception as e:
                          st.error(f"Error generating Excel file: {e}")
                          logger.error(f"Excel export error: {e}")

        else:
            st.info("Scrape some data first to enable export options.")


if __name__ == "__main__":
    # Ensure necessary library for Excel export is available
    try:
        import openpyxl
    except ImportError:
        st.error("The 'openpyxl' library is required for Excel export. Please install it (`pip install openpyxl`).")
        st.stop() # Stop execution if library is missing

    main()
