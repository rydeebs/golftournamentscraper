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
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

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
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Referer': 'https://www.google.com/'  # Add a referer to look more like a real browser
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

def get_page_html_with_browser(url, timeout=15):
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.set_page_load_timeout(timeout)
        driver.get(url)
        # Wait for JavaScript to render content
        time.sleep(3)
        html_content = driver.page_source
        driver.quit()
        return html_content
    except Exception as e:
        logger.error(f"Error fetching {url} with browser: {str(e)}")
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
    if not relative_url: return None
    relative_url = str(relative_url).strip()
    if not base_url and not relative_url.startswith(('http://', 'https://')):
        logger.warning(f"Cannot construct absolute URL: base_url is missing and relative_url '{relative_url}' is not absolute.")
        return relative_url
    if relative_url.startswith(('http://', 'https://')): return relative_url
    if relative_url.startswith('//'):
        if base_url and '://' in base_url: scheme = base_url.split('://')[0]
        else: scheme = 'https'
        return f"{scheme}:{relative_url}"
    if relative_url.startswith(('javascript:', 'mailto:', 'tel:', '#')): return None
    if not base_url:
        logger.warning(f"base_url not provided for relative_url: {relative_url}. Cannot resolve.")
        return None
    try: return urljoin(base_url, relative_url)
    except ValueError as e:
        logger.error(f"Error joining base_url '{base_url}' with relative_url '{relative_url}': {e}")
        return None

def parse_date(date_string):
    if not date_string: return None
    date_string = str(date_string).strip()
    if date_string.lower() in ['tbd', 'tba', '']: return 'TBD'
    date_formats = ['%m/%d/%Y', '%m/%d/%y', '%B %d, %Y', '%b %d, %Y', '%Y-%m-%d', '%m-%d-%Y', '%d %B %Y', '%d %b %Y']
    if ' - ' in date_string: date_string = date_string.split(' - ')[0].strip()
    elif re.match(r'\w+\s\d+-\d+', date_string): date_string = date_string.split('-')[0].strip()
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
            if re.match(r'^[A-Za-z]+\s+\d+$', date_candidate):
                year_match_in_text = re.search(r'\b(20\d{2})\b', text)
                if year_match_in_text: date_candidate += f", {year_match_in_text.group(1)}"
            return date_candidate
    return None

def extract_location(text):
    location_info = {'city': None, 'state': None, 'zip': None}
    if not text: return location_info
    text = str(text).strip()
    match = re.search(r'([A-Za-z\s\.-]+?),\s*([A-Z]{2})\s*(\d{5}(?:-\d{4})?)?', text)
    if match:
        location_info['city'] = match.group(1).strip().rstrip(',.'); location_info['state'] = match.group(2).strip()
        location_info['zip'] = match.group(3).strip() if match.group(3) else None; return location_info
    match = re.search(r'([A-Za-z\s\.-]+?),\s*([A-Za-z]{3,})\s*(\d{5}(?:-\d{4})?)?', text)
    if match:
        psn = match.group(2).strip()
        if len(psn) > 2 or (len(psn) == 2 and psn.isupper()):
             location_info['city'] = match.group(1).strip().rstrip(',.'); location_info['state'] = psn
             location_info['zip'] = match.group(3).strip() if match.group(3) else None; return location_info
    match = re.search(r'([A-Za-z\s\.-]+?),\s*([A-Z]{2})\b', text)
    if match:
        location_info['city'] = match.group(1).strip().rstrip(',.'); location_info['state'] = match.group(2).strip()
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
                    if best_match_str is None or len(candidate) > len(best_match_str): best_match_str = candidate
    if best_match_str:
        best_match_str = re.sub(r'[\s,.-]+$', '', best_match_str); best_match_str = re.sub(r'^\d{4}\s+', '', best_match_str).strip()
        loc_check = extract_location(best_match_str)
        if loc_check['city'] and loc_check['state'] and loc_check['city'].lower() == best_match_str.lower().split(',')[0].strip().lower(): return None
        return best_match_str
    return None

def is_valid_tournament_name(name_text, course_name=None, city_name=None, state_name=None):
    if not name_text: return False
    name_text_stripped = str(name_text).strip()
    if len(name_text_stripped) < 3: return False
    name_lower = name_text_stripped.lower()
    NON_TOURNAMENT_KEYWORDS = [
        "create an account", "printable schedules", "log in", "about us", "contact us", "view leaderboard", "leaderboard", "results",
        "tee times", "rankings", "standings", "store", "information", "details", "rules", "policies", "format", "golf genius",
        "registration", "sign up", "membership", "home", "schedule", "news", "bluegolf", "photos", "videos", "sponsors",
        "directions", "map", "faq", "archive", "fsga", "click here", "read more", "learn more", "welcome", "important notice",
        "usga", "please wait", "past events", "upcoming events", "all events", "event calendar", "my account", "profile",
        "settings", "dashboard", "manage account", "pga", "course handicap", "handicap index", "course rating", "slope rating",
        "lpga", "weather", "forecast", "terms of use", "privacy policy", "site map", "powered by", "official website",
        "enter a tournament", "show all", "advanced search", "post a score", "tee time reservation", "book now", "add to cart",
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
    if re.fullmatch(r'\d{1,2}/\d{1,2}/\d{2,4}', name_text_stripped) or \
       re.fullmatch(r'(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s*\d{1,2}(?:,\s*\d{4})?', name_lower) : return False
    if not has_strong_indicator and (name_lower.endswith(" gc") or name_lower.endswith(" cc") or name_lower.endswith(" golf club") or name_lower.endswith(" country club")):
        if course_name and name_lower in course_name.lower(): return False
    if name_lower in ["events", "tournaments", "schedule of events", "tournament schedule"]: return False
    if re.fullmatch(r'\d{4}', name_text_stripped): return False
    if re.fullmatch(r'\d+', name_text_stripped) or (len(name_text_stripped.split()) == 1 and len(name_text_stripped) < 5 and not has_strong_indicator): return False
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
    if not url: return 'generic'
    url_lower = url.lower()
    if 'fsga.org' in url_lower: return 'fsga'
    if 'golfgenius.com' in url_lower: return 'golfgenius'
    if 'bluegolf.com' in url_lower: return 'bluegolf'
    if 'amateurgolf.com' in url_lower: return 'amateurgolf'
    if html:
        html_lower = html.lower()
        if re.search(r'florida state golf association|fsga', html_lower): return 'fsga'
        if re.search(r'golfgenius|golf genius', html_lower): return 'golfgenius'
        if re.search(r'bluegolf|blue golf|\'bluegolf\.com\'', html_lower): return 'bluegolf'
        if re.search(r'amateurgolf\.com', html_lower): return 'amateurgolf'
    return 'generic'

def parse_generic_tournament_item(element, base_url, site_type='generic'):
    tournament_data = initialize_tournament_data(); used_cell_indices = set()
    if element.name == 'tr':
        cells = element.find_all(['td', 'th'], recursive=False);
        if len(cells) < 1: return None
        cell_contents = [{'index': idx, 'text': ct.get_text(separator=' ', strip=True), 'tag': ct,
                          'link_url': lt['href'] if (lt := ct.find('a', href=True)) else None,
                          'link_text': lt.get_text(strip=True) if lt else None} for idx, ct in enumerate(cells)]
        for ci in cell_contents:
            if ci['index'] in used_cell_indices: continue
            cd = extract_clean_date_string(ci['text'])
            if cd: tournament_data['Original Date'] = cd;
            if len(cd or "") > len(ci['text']) * 0.5 or len(ci['text']) < 25: used_cell_indices.add(ci['index']); break
        if not tournament_data['Original Date']:
            for ci in cell_contents: 
                cd = extract_clean_date_string(ci['text'])
                if cd: 
                    tournament_data['Original Date'] = cd
                    break
        for ci in cell_contents:
            if ci['index'] in used_cell_indices: continue; crs = extract_golf_course(ci['text'])
            if crs: tournament_data['Course'] = crs; used_cell_indices.add(ci['index']); break
        if not tournament_data['Course']:
            for ci in cell_contents: 
                crs = extract_golf_course(ci['text'])
                if crs: 
                    tournament_data['Course'] = crs
                    break
        for ci in cell_contents:
            if ci['index'] in used_cell_indices: continue; loc = extract_location(ci['text'])
            if loc['city'] and loc['state']: tournament_data.update(loc); used_cell_indices.add(ci['index']); break
        if not tournament_data['City']:
            for ci in cell_contents: 
                loc = extract_location(ci['text'])
                if loc['city'] and loc['state']: 
                    tournament_data.update(loc)
                    break
        name_candidates = []
        for ci in cell_contents:
            if ci['index'] in used_cell_indices and not name_candidates: continue
            tfn = ci['link_text'] if ci['link_text'] else ci['text']
            tfn = re.sub(r'^(?:View\s)?(?:Leaderboard|Results|Details|Info|Tee Times|Register|Enter)\s*[-–]?\s*', '', tfn, flags=re.I).strip()
            tfn = re.sub(r'\s*[-–]?\s*(?:Leaderboard|Results|Details|Info|Tee Times|Register|Enter)$', '', tfn, flags=re.I).strip()
            tfn = re.sub(r'\s?\*FULL\*$', '', tfn, flags=re.I).strip()
            if is_valid_tournament_name(tfn, tournament_data['Course'], tournament_data['City'], tournament_data['State']):
                au = construct_absolute_url(base_url, ci['link_url']); score = len(tfn) + (20 if au else 0)
                if tournament_data['Course'] and tournament_data['Course'].lower() in tfn.lower(): score -=10
                name_candidates.append({'name': tfn, 'url': au, 'score': score})
        if name_candidates:
            name_candidates.sort(key=lambda x: x['score'], reverse=True)
            tournament_data['Name'] = name_candidates[0]['name']; tournament_data['Detail URL'] = name_candidates[0]['url']
        else:
            ltxt = "";
            for ci in cell_contents:
                if ci['index'] not in used_cell_indices and len(ci['text']) > len(ltxt):
                    if is_valid_tournament_name(ci['text'],tournament_data['Course'],tournament_data['City'],tournament_data['State']):
                        ltxt = ci['text']; tournament_data['Detail URL'] = construct_absolute_url(base_url, ci['link_url'])
            if ltxt: tournament_data['Name'] = ltxt
    else:
        etxt = element.get_text(separator=' ', strip=True)
        tournament_data['Original Date'] = extract_clean_date_string(etxt); tournament_data['Course'] = extract_golf_course(etxt)
        loc_info = extract_location(etxt); tournament_data.update(loc_info)
        ncand = etxt; link = element.find('a', href=True); urlcand = None
        if link: ltxt_ = link.get_text(strip=True); # Renamed to avoid conflict
        if ltxt_: ncand = ltxt_; urlcand = construct_absolute_url(base_url, link.get('href')) # Use renamed ltxt_
        ncand = re.sub(r'^(?:View\s)?(?:Leaderboard|Results|Details|Info|Tee Times|Register|Enter)\s*[-–]?\s*', '', ncand, flags=re.I).strip()
        ncand = re.sub(r'\s*[-–]?\s*(?:Leaderboard|Results|Details|Info|Tee Times|Register|Enter)$', '', ncand, flags=re.I).strip()
        ncand = re.sub(r'\s?\*FULL\*$', '', ncand, flags=re.I).strip()
        if is_valid_tournament_name(ncand, tournament_data['Course'], tournament_data['City'], tournament_data['State']):
            tournament_data['Name'] = ncand; tournament_data['Detail URL'] = urlcand
        elif is_valid_tournament_name(etxt, tournament_data['Course'], tournament_data['City'], tournament_data['State']):
             tournament_data['Name'] = re.sub(r'\s?\*FULL\*$', '', etxt, flags=re.I).strip(); tournament_data['Detail URL'] = urlcand
    if not tournament_data['Name'] or not tournament_data['Original Date']: return None
    tournament_data['Date'] = parse_date(tournament_data['Original Date'])
    tournament_data['Is Qualifier'] = is_qualifier_tournament(tournament_data['Name'])
    if tournament_data['Is Qualifier']: tournament_data['Tournament Type'] = 'Qualifying Round'
    tournament_data['Tournament ID'] = generate_tournament_id(tournament_data)
    return tournament_data

# CORRECTED parse_golfgenius_detail_page
def parse_golfgenius_detail_page(soup, url):
    tournament_data = initialize_tournament_data()
    tournament_data['Detail URL'] = url

    name_text_candidate = None # Initialize candidate
    name_tag = soup.select_one('h1.text-white, h1.custom-event-title, h1#event-title, h1.event-title, h1')
    if name_tag:
        name_text_candidate = name_tag.get_text(strip=True)
    
    if name_text_candidate and is_valid_tournament_name(name_text_candidate):
        tournament_data['Name'] = name_text_candidate
    
    date_elements = soup.select('.gg-event-header-date, .event-date, .portlet-event-date'); date_text_found = None
    if date_elements: date_text_found = date_elements[0].get_text(strip=True)
    else:
        all_text = soup.get_text(" ", strip=True)
        date_match = re.search(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:(?:st|nd|rd|th)?(?:,\s*\d{4})?)?|\b\d{1,2}/\d{1,2}/\d{2,4}\b', all_text, re.I)
        if date_match: date_text_found = date_match.group(0)
    
    if date_text_found:
        tournament_data['Original Date'] = extract_clean_date_string(date_text_found)
        tournament_data['Date'] = parse_date(tournament_data['Original Date'])
    
    host_facility_header = soup.find(['h2','h3','strong'], string=re.compile("Host Facilit", re.I)); course_location_text = None
    if host_facility_header: 
        next_elem = host_facility_header.find_next_sibling()
        if next_elem: course_location_text = next_elem.get_text(" ", strip=True)
    else:
        potential_course_tags = soup.find_all(string=re.compile(r'Golf Club|Country Club|Links|National|Resort', re.I))
        if potential_course_tags:
            for tag_ in potential_course_tags: # Use different variable name for the loop variable
                parent_block = tag_.find_parent('div', limit=3) # Use tag_
                if parent_block: course_location_text = parent_block.get_text(" ", strip=True); break
        if not course_location_text: course_location_text = soup.get_text(" ", strip=True)
    
    if course_location_text:
        tournament_data['Course'] = extract_golf_course(course_location_text)
        loc_info = extract_location(course_location_text); tournament_data.update(loc_info)
    
    if not tournament_data['Name'] or not tournament_data['Original Date']:
        logger.warning(f"Could not extract essential Name/Date from GolfGenius detail page: {url}"); return None
    
    tournament_data['Tournament ID'] = generate_tournament_id(tournament_data)
    return tournament_data

def extract_schedule_tournaments_base(soup, url, site_type, max_details, show_progress, progress_bar, progress_text):
    tournaments = []; processed_ids = set(); tournament_elements = []
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
    if not tournament_elements and site_type == 'generic':
        secondary_selectors = ['li', 'div.row', 'div.card']
        for selector in secondary_selectors:
            elements = soup.select(selector)
            elements = [el for el in elements if len(el.get_text(strip=True)) > 30 and el.find('a')]
            if elements: tournament_elements.extend(elements); logger.info(f"Site {site_type}: Found {len(elements)} elements with fallback selector '{selector}' for {url}")
    if not tournament_elements:
        logger.warning(f"No potential tournament elements found on {url} for site type {site_type} with primary selectors.")
        all_tables = soup.find_all('table')
        for table_ in all_tables: # Use different variable name for the loop variable
            rows = table_.find_all('tr'); rows_with_td = [row for row in rows if row.find('td')]
            if len(rows_with_td) > 0 : tournament_elements.extend(rows_with_td)
        if tournament_elements: logger.info(f"Found {len(tournament_elements)} rows by checking all tables as a last resort.")
    total_elements = len(tournament_elements)
    for i, element in enumerate(tournament_elements):
        if show_progress and progress_bar and total_elements > 0:
            progress_bar.progress((i + 1) / total_elements)
            if progress_text: progress_text.text(f"Processing {site_type} item {i+1} of {total_elements}...")
        tournament = parse_generic_tournament_item(element, url, site_type=site_type)
        if tournament and tournament['Tournament ID'] not in processed_ids:
            tournaments.append(tournament); processed_ids.add(tournament['Tournament ID'])
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
    cache_key_version = "v2.4"; cache_key = f"schedule_{cache_key_version}_{hashlib.md5(url.encode()).hexdigest()}" # Incremented cache key
    cached_data = load_from_cache(cache_key)
    if cached_data:
        if show_progress: st.success(f"Loaded {len(cached_data)} tournaments from cache for {url}.")
        for item in cached_data: defaults = initialize_tournament_data();
        for key_def in defaults: item.setdefault(key_def, defaults[key_def]); return cached_data
    html = get_page_html(url)
    if not html:
        return []
    
    # Debug: Save the HTML to a file to inspect
    with open("debug_response.html", "w", encoding="utf-8") as f:
        f.write(html)
    
    soup = BeautifulSoup(html, 'html.parser'); tournaments = []
    if show_progress:
        progress_text = st.empty(); progress_bar = st.progress(0.0)
        progress_text.text(f"Analyzing page and finding tournaments from {url}...")
    site_type = detect_site_type(url, html); logger.info(f"Detected site type: {site_type} for URL: {url}")
    if site_type == 'fsga': tournaments = extract_fsga_schedule_tournaments(soup, url, max_details, show_progress, progress_bar, progress_text)
    elif site_type == 'golfgenius': tournaments = extract_golfgenius_schedule_tournaments(soup, url, max_details, show_progress, progress_bar, progress_text)
    elif site_type == 'bluegolf': tournaments = extract_bluegolf_schedule_tournaments(soup, url, max_details, show_progress, progress_bar, progress_text)
    else: tournaments = extract_generic_schedule_tournaments(soup, url, max_details, show_progress, progress_bar, progress_text)
    if site_type == 'golfgenius' and "/pages/" in url.lower() and not tournaments: # Check if schedule parse failed for a detail page
        logger.info(f"GolfGenius URL {url} looks like a detail page and schedule parse failed. Attempting detail page parse.")
        tournament_detail = parse_golfgenius_detail_page(soup, url)
        if tournament_detail: tournaments.append(tournament_detail); logger.info(f"Successfully parsed GolfGenius detail page: {tournament_detail.get('Name')}")
    if tournaments:
        for item in tournaments: defaults = initialize_tournament_data();
        for key_def in defaults: item.setdefault(key_def, defaults[key_def]); save_to_cache(cache_key, tournaments)
    if show_progress:
        progress_bar.progress(1.0); progress_text.text(f"Found {len(tournaments)} valid tournament entries from {url}.")
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
                        results = scrape_tournaments(url, max_details=0, show_progress=True)
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