# START OF FILE app (1).py

import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
import csv
import io
import base64
from urllib.parse import urljoin # Still used, but with pre-checks
import logging
from datetime import datetime, timedelta
import time
import pandas as pd
import json
import hashlib
import os
# from concurrent.futures import ThreadPoolExecutor, as_completed # Not used yet

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Helper Functions (CSV/JSON Download, Cache, HTTP Get) ---
def get_table_download_link(data, filename="tournament_data.csv"):
    if not data: return ""
    if not isinstance(data, list) or (data and not isinstance(data[0], dict)):
         logger.error("Data for CSV export is not a list of dictionaries.")
         if isinstance(data, pd.DataFrame): data = data.to_dict('records')
         else: return "<p>Error: Invalid data format for CSV export.</p>"
    try:
        df = pd.DataFrame(data)
        desired_cols = ['Original Date', 'Name', 'Course', 'City', 'State', 'Zip']
        actual_cols = df.columns.tolist()
        final_cols = [col for col in desired_cols if col in actual_cols]
        other_cols = [col for col in actual_cols if col not in final_cols]
        df = df[final_cols + other_cols]
        csv_string = df.to_csv(index=False)
        b64 = base64.b64encode(csv_string.encode()).decode()
        return f'<a href="data:file/csv;base64,{b64}" download="{filename}">Download CSV file</a>'
    except Exception as e:
        logger.error(f"Error generating CSV link: {e}")
        return f"<p>Error generating CSV: {e}</p>"

def get_json_download_link(data, filename="tournament_data.json"):
    if not data: return ""
    try:
        json_string = json.dumps(data, indent=2)
        b64 = base64.b64encode(json_string.encode()).decode()
        return f'<a href="data:file/json;base64,{b64}" download="{filename}">Download JSON file</a>'
    except Exception as e:
        logger.error(f"Error generating JSON link: {e}")
        return f"<p>Error generating JSON: {e}</p>"

def get_page_html(url, timeout=15, max_retries=3):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9', 'Connection': 'keep-alive'
    }
    retry_count = 0
    while retry_count < max_retries:
        try:
            if not url.startswith(('http://', 'https://')): url = 'https://' + url
            response = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            response.raise_for_status()
            try: html_content = response.content.decode(response.apparent_encoding)
            except (UnicodeDecodeError, LookupError): html_content = response.content.decode('utf-8', errors='replace')
            return html_content
        except requests.RequestException as e:
            retry_count += 1
            error_message = f"Error fetching {url}: {str(e)}"
            logger.warning(f"Attempt {retry_count} failed: {error_message}")
            if retry_count >= max_retries:
                st.error(f"Failed to fetch data from {url} after {max_retries} attempts. Error: {str(e)}")
                return None
            time.sleep(1)
    return None

def save_to_cache(key, data):
    cache_dir = ".cache"
    if not os.path.exists(cache_dir): os.makedirs(cache_dir)
    cache_file = os.path.join(cache_dir, f"{key}.json")
    try:
        with open(cache_file, 'w', encoding='utf-8') as f: json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e: logger.error(f"Failed to save cache file {cache_file}: {e}")

def load_from_cache(key, max_age_hours=24):
    cache_dir = ".cache"; cache_file = os.path.join(cache_dir, f"{key}.json")
    if not os.path.exists(cache_file): return None
    try:
        file_age = time.time() - os.path.getmtime(cache_file)
        if file_age > (max_age_hours * 3600):
            logger.info(f"Cache expired for key: {key}"); return None
    except OSError as e: logger.error(f"Error checking cache file age {cache_file}: {e}"); return None
    try:
        with open(cache_file, 'r', encoding='utf-8') as f:
            logger.info(f"Loading data from cache for key: {key}"); return json.load(f)
    except (json.JSONDecodeError, IOError, UnicodeDecodeError) as e:
        logger.error(f"Error loading cache file {cache_file}: {e}")
        try: os.remove(cache_file); logger.info(f"Removed potentially corrupted cache file: {cache_file}")
        except OSError as de: logger.error(f"Failed to remove corrupted cache file {cache_file}: {de}")
        return None

# --- Parsing and Extraction Functions ---
def initialize_tournament_data():
    return {
        'Original Date': None, 'Name': None, 'Course': None, 'City': None, 'State': None, 'Zip': None,
        'Date': None, 'Detail URL': None, 'Tournament ID': None, 'Is Qualifier': False,
        'Has Qualifiers': None, 'Qualifier Count': None, 'Parent Tournament': None,
        'Parent Tournament ID': None, 'Eligibility': None, 'Description': None, 'Tournament Type': None,
    }

def construct_absolute_url(base_url, relative_url):
    """Safely constructs an absolute URL from a base URL and a relative URL."""
    if not relative_url: return None
    relative_url = str(relative_url).strip()
    if not base_url and not relative_url.startswith(('http://', 'https://')):
        logger.warning(f"Cannot construct absolute URL: base_url is missing and relative_url '{relative_url}' is not absolute.")
        return relative_url # Return as is, might be an error or a fragment

    # If relative_url is already absolute
    if relative_url.startswith(('http://', 'https://')):
        return relative_url
    # Handle protocol-relative URLs (e.g., //example.com/path)
    if relative_url.startswith('//'):
        if base_url and '://' in base_url:
            scheme = base_url.split('://')[0]
            return f"{scheme}:{relative_url}"
        else: # Default to https if base_url scheme is unknown
            return f"https:{relative_url}"
    # Ignore javascript, mailto, tel, anchor links meant for same page
    if relative_url.startswith(('javascript:', 'mailto:', 'tel:', '#')):
        return None # Or return base_url + relative_url if # is for same page navigation

    # If base_url is not provided but relative_url is just a path (should not happen ideally)
    if not base_url:
        logger.warning(f"base_url not provided for relative_url: {relative_url}. Cannot resolve.")
        return None # Cannot resolve without a base

    try:
        return urljoin(base_url, relative_url)
    except ValueError as e:
        logger.error(f"Error joining base_url '{base_url}' with relative_url '{relative_url}': {e}")
        return None # Or return relative_url to indicate failure

def parse_date(date_string):
    if not date_string: return None
    date_string = str(date_string).strip()
    if date_string.lower() in ['tbd', 'tba', '']: return 'TBD'
    date_formats = [
        '%m/%d/%Y', '%m/%d/%y', '%B %d, %Y', '%b %d, %Y', '%Y-%m-%d',
        '%m-%d-%Y', '%d %B %Y', '%d %b %Y',
    ]
    # Simpler range handling: try to parse the start if obvious
    if ' - ' in date_string: date_string = date_string.split(' - ')[0].strip()
    elif re.match(r'\w+\s\d+-\d+', date_string): # "May 19-21"
         parts = date_string.split('-')
         date_string = parts[0].strip()
         # if year is missing and present later in original string, it should be caught by extract_clean_date_string

    for date_format in date_formats:
        try:
            parsed_date = datetime.strptime(date_string, date_format)
            if parsed_date.year < 100:
                current_year = datetime.now().year; century = (current_year // 100) * 100
                parsed_year = century + parsed_date.year
                if parsed_year > current_year + 10: parsed_year -= 100
                parsed_date = parsed_date.replace(year=parsed_year)
            return parsed_date.strftime('%Y-%m-%d')
        except (ValueError, IndexError): continue
    logger.warning(f"Could not parse date: {date_string}. Returning original.")
    return date_string

def extract_clean_date_string(text):
    if not text: return None
    text = str(text).strip()
    patterns = [
        r'\b(\d{1,2}/\d{1,2}/\d{2,4})\b',
        r'\b((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:st|nd|rd|th)?(?:,\s*\d{2,4})?)\b',
        r'\b(\d{4}-\d{2}-\d{2})\b'
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            date_candidate = match.group(1).strip()
            # If "Month Day" format without year, try to find a year in the original text
            if re.match(r'^[A-Za-z]+\s+\d+$', date_candidate):
                year_match_in_text = re.search(r'\b(20\d{2})\b', text) # Look for a 4-digit year
                if year_match_in_text:
                    date_candidate += f", {year_match_in_text.group(1)}"
            return date_candidate
    return None

def extract_location(text):
    location_info = {'city': None, 'state': None, 'zip': None}
    if not text: return location_info
    text = str(text).strip()
    match = re.search(r'([A-Za-z\s\.-]+?),\s*([A-Z]{2})\s*(\d{5}(?:-\d{4})?)?', text)
    if match:
        location_info['city'] = match.group(1).strip().rstrip(',.')
        location_info['state'] = match.group(2).strip()
        location_info['zip'] = match.group(3).strip() if match.group(3) else None
        return location_info
    match = re.search(r'([A-Za-z\s\.-]+?),\s*([A-Za-z]{3,})\s*(\d{5}(?:-\d{4})?)?', text)
    if match:
        potential_state_name = match.group(2).strip()
        if len(potential_state_name) > 2 or (len(potential_state_name) == 2 and potential_state_name.isupper()):
             location_info['city'] = match.group(1).strip().rstrip(',.')
             location_info['state'] = potential_state_name
             location_info['zip'] = match.group(3).strip() if match.group(3) else None
             return location_info
    match = re.search(r'([A-Za-z\s\.-]+?),\s*([A-Z]{2})\b', text)
    if match:
        location_info['city'] = match.group(1).strip().rstrip(',.')
        location_info['state'] = match.group(2).strip()
        return location_info
    return location_info

def extract_golf_course(text):
    if not text: return None
    text = str(text).strip()
    patterns = [
        r'(?:at|venue|course|host site)\s*[:\-]?\s*([\w\s.&\'\-]+(?:Golf\s*Club|Country\s*Club|Golf\s*Course|G\.?C\.?C?\.?|Golf|Links|Course|Club|Plantation|Preserve|National|Resort|Park|Center| Dunes)\b)',
        r'\b([\w\s.&\'\-]+(?:Golf\s*Club|Country\s*Club|Golf\s*Course|G\.?C\.?C?\.?|Golf|Links|Course|Club|Plantation|Preserve|National|Resort|Park|Center| Dunes))\b'
    ]
    best_match_str = None
    for pattern in patterns:
        matches = re.finditer(pattern, text, re.I)
        for match_obj in matches:
            candidate = match_obj.group(1).strip()
            if ',' not in candidate and len(candidate) > 3 and not candidate.lower().endswith((" (east)", " (west)", " (north)", " (south)")):
                if not re.fullmatch(r'[A-Za-z]{2,}', candidate) or any(kw in candidate.lower() for kw in ["golf", "club", "course", "links", "country", "resort", "national", "center"]):
                    if best_match_str is None or len(candidate) > len(best_match_str):
                        best_match_str = candidate
    if best_match_str:
        best_match_str = re.sub(r'[\s,.-]+$', '', best_match_str)
        best_match_str = re.sub(r'^\d{4}\s+', '', best_match_str).strip() # Remove leading year "2025 Course"
        # Avoid returning just city or state if it accidentally matched course keywords
        loc_check = extract_location(best_match_str)
        if loc_check['city'] and loc_check['state'] and loc_check['city'].lower() == best_match_str.lower().split(',')[0].strip().lower():
            return None # It's likely just a location string
        return best_match_str
    return None

def is_valid_tournament_name(name_text, course_name=None, city_name=None, state_name=None):
    if not name_text or len(name_text.strip()) < 3: return False
    name_lower = name_text.lower().strip()
    NON_TOURNAMENT_KEYWORDS = [
        "create an account", "printable schedules", "log in", "about us", "contact us",
        "view leaderboard", "leaderboard", "results", "tee times", "rankings", "standings",
        "store", "information", "details", "rules", "policies", "format", "golf genius",
        "registration", "sign up", "membership", "home", "schedule", "news", "bluegolf",
        "photos", "videos", "sponsors", "directions", "map", "faq", "archive", "fsga",
        "click here", "read more", "learn more", "welcome", "important notice", "usga",
        "please wait", "past events", "upcoming events", "all events", "event calendar",
        "my account", "profile", "settings", "dashboard", "manage account", "pga",
        "course handicap", "handicap index", "course rating", "slope rating", "lpga",
        "weather", "forecast", "terms of use", "privacy policy", "site map",
        "powered by", "official website", "enter a tournament", "show all", "advanced search",
        "post a score", "tee time reservation", "book now", "add to cart",
        "select a page", "no events found", "tournament portal", "tournament central",
    ]
    for keyword in NON_TOURNAMENT_KEYWORDS:
        if keyword == name_lower or name_lower.startswith(keyword + " ") or name_lower.endswith(" " + keyword):
            if keyword in ["juniors", "men's", "women's", "senior", "amateur"] and len(name_lower.split()) > 2: continue
            return False
    strong_tournament_indicators = ["championship", "open", "classic", "invitational", "cup", "trophy", "tour", "event", "qualifier", "challenge", "series", "foursomes", "scramble", "inv"]
    has_strong_indicator = any(indicator in name_lower for indicator in strong_tournament_indicators)

    if course_name and name_lower == course_name.lower() and not has_strong_indicator: return False
    if city_name and name_lower == city_name.lower() and not has_strong_indicator: return False
    if state_name and name_lower == state_name.lower() and not has_strong_indicator: return False
        
    if re.fullmatch(r'\d{1,2}/\d{1,2}/\d{2,4}', name_text) or \
       re.fullmatch(r'(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s*\d{1,2}(?:,\s*\d{4})?', name_lower) : return False
    if not has_strong_indicator and (name_lower.endswith(" gc") or name_lower.endswith(" cc") or name_lower.endswith(" golf club") or name_lower.endswith(" country club")):
        if course_name and name_lower in course_name.lower(): return False
    if name_lower in ["events", "tournaments", "schedule of events", "tournament schedule"]: return False
    if re.fullmatch(r'\d{4}', name_text): return False
    # Filter out if name is just a number or very short non-descriptive text
    if re.fullmatch(r'\d+', name_text) or (len(name_text.split()) == 1 and len(name_text) < 5 and not has_strong_indicator): return False
    return True

def generate_tournament_id(tournament_data):
    key_data = f"{tournament_data.get('Name', '')}-{tournament_data.get('Original Date', '')}"
    if tournament_data.get('City') and tournament_data.get('State'): key_data += f"-{tournament_data.get('City')}-{tournament_data.get('State')}"
    if tournament_data.get('Course'): key_data += f"-{tournament_data.get('Course')}"
    return hashlib.md5(key_data.encode()).hexdigest()[:12]

def is_qualifier_tournament(name):
    if not name: return False; name_lower = str(name).lower()
    return any(keyword in name_lower for keyword in ['qualifier', 'qualifying', 'q-school', 'local qualifying', 'sectional qualifying'])

def detect_site_type(url, html=None):
    if not url: return 'generic'; url_lower = url.lower()
    if 'fsga.org' in url_lower: return 'fsga'
    if 'golfgenius.com' in url_lower: return 'golfgenius' # Check .com
    if 'bluegolf.com' in url_lower: return 'bluegolf'   # Check .com
    if 'amateurgolf.com' in url_lower: return 'amateurgolf'
    if html:
        html_lower = html.lower()
        if re.search(r'florida state golf association|fsga', html_lower): return 'fsga'
        if re.search(r'golfgenius|golf genius', html_lower): return 'golfgenius'
        if re.search(r'bluegolf|blue golf|\'bluegolf\.com\'', html_lower): return 'bluegolf'
        if re.search(r'amateurgolf\.com', html_lower): return 'amateurgolf'
    return 'generic'

def parse_generic_tournament_item(element, base_url, site_type='generic'):
    tournament_data = initialize_tournament_data()
    used_cell_indices = set() # To track which cells provided primary data

    if element.name == 'tr': # Prioritize table rows
        cells = element.find_all(['td', 'th'], recursive=False) # Non-recursive to get direct children
        if len(cells) < 1: return None # Need at least one cell

        cell_contents = []
        for idx, cell_tag in enumerate(cells):
            text = cell_tag.get_text(separator=' ', strip=True)
            link_tag = cell_tag.find('a', href=True)
            cell_contents.append({
                'index': idx, 'text': text, 'tag': cell_tag,
                'link_url': link_tag['href'] if link_tag else None,
                'link_text': link_tag.get_text(strip=True) if link_tag else None
            })
        
        # 1. Extract Date
        for cell_info in cell_contents:
            if cell_info['index'] in used_cell_indices: continue
            clean_date = extract_clean_date_string(cell_info['text'])
            if clean_date:
                tournament_data['Original Date'] = clean_date
                # If cell is mostly date, mark as used for primary data
                if len(clean_date) > len(cell_info['text']) * 0.5 or len(cell_info['text']) < 25:
                    used_cell_indices.add(cell_info['index'])
                break 
        # Fallback if no dedicated date cell found
        if not tournament_data['Original Date']:
            for cell_info in cell_contents:
                clean_date = extract_clean_date_string(cell_info['text'])
                if clean_date:
                    tournament_data['Original Date'] = clean_date; break
        
        # 2. Extract Course
        for cell_info in cell_contents:
            if cell_info['index'] in used_cell_indices: continue
            course = extract_golf_course(cell_info['text'])
            if course:
                tournament_data['Course'] = course
                used_cell_indices.add(cell_info['index']); break
        if not tournament_data['Course']: # Fallback
            for cell_info in cell_contents:
                course = extract_golf_course(cell_info['text'])
                if course: tournament_data['Course'] = course; break

        # 3. Extract Location
        for cell_info in cell_contents:
            if cell_info['index'] in used_cell_indices: continue
            loc = extract_location(cell_info['text'])
            if loc['city'] and loc['state']:
                tournament_data.update(loc)
                used_cell_indices.add(cell_info['index']); break
        if not tournament_data['City']: # Fallback
            for cell_info in cell_contents:
                loc = extract_location(cell_info['text'])
                if loc['city'] and loc['state']: tournament_data.update(loc); break
        
        # 4. Extract Name and Detail URL
        name_candidates_list = []
        for cell_info in cell_contents:
            if cell_info['index'] in used_cell_indices and not name_candidates_list: continue # Only consider used cells for name if no other options

            text_for_name = cell_info['link_text'] if cell_info['link_text'] else cell_info['text']
            # Clean common non-name parts
            text_for_name = re.sub(r'^(?:View\s)?(?:Leaderboard|Results|Details|Info|Tee Times|Register|Enter)\s*[-–]?\s*', '', text_for_name, flags=re.I).strip()
            text_for_name = re.sub(r'\s*[-–]?\s*(?:Leaderboard|Results|Details|Info|Tee Times|Register|Enter)$', '', text_for_name, flags=re.I).strip()
            text_for_name = re.sub(r'\s?\*FULL\*$', '', text_for_name, flags=re.I).strip()
            
            if is_valid_tournament_name(text_for_name, tournament_data['Course'], tournament_data['City'], tournament_data['State']):
                abs_url = construct_absolute_url(base_url, cell_info['link_url'])
                # Score candidate: prefer links, longer names, and not matching course/location too closely
                score = len(text_for_name)
                if abs_url: score += 20
                if tournament_data['Course'] and tournament_data['Course'].lower() in text_for_name.lower(): score -=10
                name_candidates_list.append({'name': text_for_name, 'url': abs_url, 'score': score})
        
        if name_candidates_list:
            name_candidates_list.sort(key=lambda x: x['score'], reverse=True)
            tournament_data['Name'] = name_candidates_list[0]['name']
            tournament_data['Detail URL'] = name_candidates_list[0]['url']
        else: # Last attempt: find longest text in a non-used cell
            longest_text = ""
            for cell_info in cell_contents:
                if cell_info['index'] not in used_cell_indices and len(cell_info['text']) > len(longest_text):
                    if is_valid_tournament_name(cell_info['text'], tournament_data['Course'], tournament_data['City'], tournament_data['State']):
                        longest_text = cell_info['text']
                        tournament_data['Detail URL'] = construct_absolute_url(base_url, cell_info['link_url'])
            if longest_text: tournament_data['Name'] = longest_text


    else: # Non-table row elements (div, li, etc.)
        element_text = element.get_text(separator=' ', strip=True)
        tournament_data['Original Date'] = extract_clean_date_string(element_text)
        tournament_data['Course'] = extract_golf_course(element_text)
        loc_info = extract_location(element_text); tournament_data.update(loc_info)
        
        name_candidate = element_text
        link = element.find('a', href=True)
        url_candidate = None
        if link:
            link_text = link.get_text(strip=True)
            if link_text: name_candidate = link_text
            url_candidate = construct_absolute_url(base_url, link.get('href'))

        name_candidate = re.sub(r'^(?:View\s)?(?:Leaderboard|Results|Details|Info|Tee Times|Register|Enter)\s*[-–]?\s*', '', name_candidate, flags=re.I).strip()
        name_candidate = re.sub(r'\s*[-–]?\s*(?:Leaderboard|Results|Details|Info|Tee Times|Register|Enter)$', '', name_candidate, flags=re.I).strip()
        name_candidate = re.sub(r'\s?\*FULL\*$', '', name_candidate, flags=re.I).strip()

        if is_valid_tournament_name(name_candidate, tournament_data['Course'], tournament_data['City'], tournament_data['State']):
            tournament_data['Name'] = name_candidate
            tournament_data['Detail URL'] = url_candidate
        # Fallback: try element_text if link text was invalid but element_text is
        elif is_valid_tournament_name(element_text, tournament_data['Course'], tournament_data['City'], tournament_data['State']):
             tournament_data['Name'] = re.sub(r'\s?\*FULL\*$', '', element_text, flags=re.I).strip() # Clean full here too
             tournament_data['Detail URL'] = url_candidate # Keep url from link if present

    if not tournament_data['Name'] or not tournament_data['Original Date']:
        return None
    tournament_data['Date'] = parse_date(tournament_data['Original Date'])
    tournament_data['Is Qualifier'] = is_qualifier_tournament(tournament_data['Name'])
    if tournament_data['Is Qualifier']: tournament_data['Tournament Type'] = 'Qualifying Round'
    tournament_data['Tournament ID'] = generate_tournament_id(tournament_data)
    return tournament_data

def extract_schedule_tournaments_base(soup, url, site_type, max_details, show_progress, progress_bar, progress_text):
    tournaments = []; processed_ids = set(); tournament_elements = []
    # Site-specific selectors can be refined here
    if site_type == 'golfgenius': selectors = ['table.search-results-table tr','div.event-list-item', 'div.list-group-item', 'table tr']
    elif site_type == 'bluegolf': selectors = ['table.scheduleCondensed tr', 'tr.tournamentItem', 'div.eventItem', 'table tr']
    elif site_type == 'fsga': selectors = ['div.tournament-list-item', 'table.dataTable tbody tr', 'div.card.tournament-card', 'table tr']
    else: selectors = ['table tr', 'li.event', 'div.event', 'div.item', 'article.event', 'div.tournament-item']

    for selector in selectors:
        elements = soup.select(selector)
        if "tr" in selector: elements = [el for el in elements if el.find('td')]
        if elements:
            tournament_elements.extend(elements)
            logger.info(f"Site {site_type}: Found {len(elements)} elements with selector '{selector}' for {url}")
            # For specific sites, sometimes the first good selector is enough, but let's allow multiple for more coverage
            # if site_type != 'generic' and len(tournament_elements) > 5 : break # Heuristic to stop if enough found

    if not tournament_elements and site_type == 'generic':
        secondary_selectors = ['li', 'div.row', 'div.card']
        for selector in secondary_selectors:
            elements = soup.select(selector)
            elements = [el for el in elements if len(el.get_text(strip=True)) > 30 and el.find('a')]
            if elements:
                tournament_elements.extend(elements)
                logger.info(f"Site {site_type}: Found {len(elements)} elements with fallback selector '{selector}' for {url}")
    
    if not tournament_elements:
        logger.warning(f"No potential tournament elements found on {url} for site type {site_type} with primary selectors.")
        # Fallback to finding any row in any table if no specific selectors worked
        all_tables = soup.find_all('table')
        for table in all_tables:
            rows = table.find_all('tr')
            rows_with_td = [row for row in rows if row.find('td')]
            if len(rows_with_td) > 0 : # Check if table has data rows
                 tournament_elements.extend(rows_with_td)
        if tournament_elements: logger.info(f"Found {len(tournament_elements)} rows by checking all tables as a last resort.")


    total_elements = len(tournament_elements)
    for i, element in enumerate(tournament_elements):
        if show_progress and progress_bar and total_elements > 0:
            progress_bar.progress((i + 1) / total_elements)
            if progress_text: progress_text.text(f"Processing {site_type} item {i+1} of {total_elements}...")
        
        # Pass the current page's URL as base_url
        tournament = parse_generic_tournament_item(element, url, site_type=site_type)
        if tournament and tournament['Tournament ID'] not in processed_ids:
            tournaments.append(tournament)
            processed_ids.add(tournament['Tournament ID'])
    logger.info(f"Extracted {len(tournaments)} unique tournaments from {site_type} schedule at {url}.")
    return tournaments

def extract_golfgenius_schedule_tournaments(soup, url, max_details=None, show_progress=True, progress_bar=None, progress_text=None):
    return extract_schedule_tournaments_base(soup, url, 'golfgenius', max_details, show_progress, progress_bar, progress_text)
def extract_bluegolf_schedule_tournaments(soup, url, max_details=None, show_progress=True, progress_bar=None, progress_text=None):
    return extract_schedule_tournaments_base(soup, url, 'bluegolf', max_details, show_progress, progress_bar, progress_text)
def extract_fsga_schedule_tournaments(soup, url, max_details=None, show_progress=True, progress_bar=None, progress_text=None):
    return extract_schedule_tournaments_base(soup, url, 'fsga', max_details, show_progress, progress_bar, progress_text)
def extract_generic_schedule_tournaments(soup, url, max_details=None, show_progress=True, progress_bar=None, progress_text=None):
    return extract_schedule_tournaments_base(soup, url, 'generic', max_details, show_progress, progress_bar, progress_text)

def scrape_tournaments(url, max_details=None, show_progress=True):
    # Use a versioned cache key if you change parsing logic significantly
    cache_key_version = "v2.1" 
    cache_key = f"schedule_{cache_key_version}_{hashlib.md5(url.encode()).hexdigest()}"
    cached_data = load_from_cache(cache_key)
    if cached_data:
        if show_progress: st.success(f"Loaded {len(cached_data)} tournaments from cache for {url}.")
        for item in cached_data:
            defaults = initialize_tournament_data()
            for key_def in defaults: item.setdefault(key_def, defaults[key_def])
        return cached_data

    html = get_page_html(url)
    if not html: return []
    soup = BeautifulSoup(html, 'html.parser')
    tournaments = []

    if show_progress:
        progress_text = st.empty(); progress_bar = st.progress(0.0)
        progress_text.text(f"Analyzing page and finding tournaments from {url}...")

    site_type = detect_site_type(url, html)
    logger.info(f"Detected site type: {site_type} for URL: {url}")

    if site_type == 'fsga': tournaments = extract_fsga_schedule_tournaments(soup, url, max_details, show_progress, progress_bar, progress_text)
    elif site_type == 'golfgenius': tournaments = extract_golfgenius_schedule_tournaments(soup, url, max_details, show_progress, progress_bar, progress_text)
    elif site_type == 'bluegolf': tournaments = extract_bluegolf_schedule_tournaments(soup, url, max_details, show_progress, progress_bar, progress_text)
    else: tournaments = extract_generic_schedule_tournaments(soup, url, max_details, show_progress, progress_bar, progress_text)

    if tournaments:
        for item in tournaments:
            defaults = initialize_tournament_data()
            for key_def in defaults: item.setdefault(key_def, defaults[key_def])
        save_to_cache(cache_key, tournaments)
    if show_progress:
        progress_bar.progress(1.0)
        progress_text.text(f"Found {len(tournaments)} valid tournament entries from {url}.")
    return tournaments

# --- Streamlit UI ---
def main():
    st.set_page_config(page_title="Golf Tournament Scraper", layout="wide")
    st.title("Golf Tournament Scraper"); st.markdown("Enter URL of a golf tournament **schedule page**.")
    st.sidebar.title("Configuration")
    default_urls = [
        "https://wpga-onlineregistration.golfgenius.com/pages/1264528",
        "https://www.fsga.org/TournamentCategory/EnterList/d99ad47f-2e7d-4ff4-8a32-c5b1eb315d28?year=2025&p=2",
        "https://usamtour.bluegolf.com/bluegolf/usamtour25/schedule/index.htm",
        "https://www.amateurgolf.com/amateur-golf-tournaments/Golf-Week-Amateur-Tour-Florida"
    ]
    url = st.sidebar.text_input("Tournament Schedule URL", value=default_urls[0])
    st.sidebar.markdown("Example URLs:"); 
    for ex_url in default_urls: st.sidebar.code(ex_url, language=None)
    with st.sidebar.expander("Advanced Options"):
        st.checkbox("Scrape detail pages (slower)", value=False, disabled=True)
        st.number_input("Max detail pages", min_value=1,value=10,step=1,disabled=True)
        if st.button("Clear Cache"):
            cache_dir = ".cache"
            if os.path.exists(cache_dir):
                import shutil
                try: shutil.rmtree(cache_dir); st.success("Cache cleared!")
                except Exception as e: st.error(f"Error clearing cache: {e}")
            else: st.info("Cache directory not found.")
            if 'tournaments' in st.session_state: del st.session_state.tournaments
        show_debug = st.checkbox("Show debug logs in console", value=False)
        logging.getLogger().setLevel(logging.DEBUG if show_debug else logging.INFO)

    if 'tournaments' not in st.session_state: st.session_state.tournaments = []
    tab1, tab2 = st.tabs(["Scraper Results", "Export"])
    with tab1:
        if st.button("Scrape Tournaments", type="primary"):
            if url:
                with st.spinner(f'Scraping {url}...'):
                    try:
                        results = scrape_tournaments(url, max_details=0, show_progress=True) # max_details = 0 for now
                        st.session_state.tournaments = results
                        if not results: st.warning("No valid tournament data extracted. Check URL or website structure.")
                        else: st.success(f"Processed and found {len(results)} potential tournament entries.")
                    except Exception as e:
                        st.error(f"Scraping error: {str(e)}"); logger.exception("Scraping failed:")
                        st.session_state.tournaments = []
            else: st.error("Please enter a URL.")

        if st.session_state.tournaments:
            st.subheader("Tournament Data")
            display_df = pd.DataFrame([{
                'Date': t.get('Original Date', ''), 'Name': t.get('Name', ''),
                'Course': t.get('Course', ''), 'City': t.get('City', ''),
                'State': t.get('State', ''), 'Zip': t.get('Zip', '')
            } for t in st.session_state.tournaments])
            st.dataframe(display_df, use_container_width=True, height=600)
            with st.expander("Raw Extracted Data (for debugging)"):
                 st.json(st.session_state.tournaments, expanded=False)
        elif not url: st.info("Enter URL and click 'Scrape Tournaments'.")
    with tab2:
        if st.session_state.tournaments:
            st.subheader("Export Options")
            all_cols = list(initialize_tournament_data().keys())
            def_cols = ['Original Date', 'Name', 'Course', 'City', 'State', 'Zip', 'Date', 'Detail URL']
            sel_cols = st.multiselect("Columns to export:", all_cols, [c for c in def_cols if c in all_cols])
            exp_fmt = st.radio("Export format", ["CSV", "JSON", "Excel"])
            fname = st.text_input("Filename", value="golf_data")
            exp_data = [{col: t.get(col) for col in sel_cols} for t in st.session_state.tournaments] if sel_cols else None
            if exp_data:
                if exp_fmt == "CSV": st.markdown(get_table_download_link(exp_data, f"{fname}.csv"), unsafe_allow_html=True)
                elif exp_fmt == "JSON": st.markdown(get_json_download_link(exp_data, f"{fname}.json"), unsafe_allow_html=True)
                elif exp_fmt == "Excel":
                    try:
                        xl_buf = io.BytesIO(); pd.DataFrame(exp_data).to_excel(xl_buf, index=False, engine='openpyxl')
                        b64 = base64.b64encode(xl_buf.getvalue()).decode()
                        st.markdown(f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{fname}.xlsx">Download Excel</a>', unsafe_allow_html=True)
                    except Exception as e: st.error(f"Excel export error: {e}"); logger.error(f"Excel error: {e}")
            elif sel_cols : st.warning("No data to export based on current scrape.")
            else: st.warning("Select columns for export.")

        else: st.info("Scrape data first for export options.")

if __name__ == "__main__":
    try: import openpyxl
    except ImportError: st.error("'openpyxl' needed for Excel. `pip install openpyxl`"); st.stop()
    main()
