import streamlit as st
import pandas as pd
import re
from datetime import datetime
import io
import base64
import requests
from bs4 import BeautifulSoup
import json
from openai import OpenAI

# --- Page Config ---
st.set_page_config(
    page_title="Golf Tournament Data Cleaner",
    page_icon="⛳",
    layout="wide"
)

# --- Data Cleaning Functions ---

def clean_date(date_str):
    """Clean and standardize date formats to YYYY-MM-DD."""
    if pd.isna(date_str) or str(date_str).strip() == '':
        return None
    
    date_str = str(date_str).strip()
    
    # Handle TBD/TBA
    if date_str.lower() in ['tbd', 'tba', 'n/a', 'na']:
        return 'TBD'
    
    # Handle date ranges (take the first date)
    if ' - ' in date_str:
        date_str = date_str.split(' - ')[0].strip()
    elif ' to ' in date_str.lower():
        date_str = date_str.lower().split(' to ')[0].strip()
    
    # Common date formats to try
    date_formats = [
        '%m/%d/%Y', '%m/%d/%y', '%m-%d-%Y', '%m-%d-%y',
        '%Y-%m-%d', '%Y/%m/%d',
        '%B %d, %Y', '%b %d, %Y', '%B %d %Y', '%b %d %Y',
        '%d %B %Y', '%d %b %Y', '%d %B, %Y', '%d %b, %Y',
        '%B %d', '%b %d',  # Without year
    ]
    
    for fmt in date_formats:
        try:
            parsed_date = datetime.strptime(date_str, fmt)
            # If year is missing or very old, assume current/next year
            if parsed_date.year == 1900 or fmt in ['%B %d', '%b %d']:
                current_year = datetime.now().year
                parsed_date = parsed_date.replace(year=current_year)
                # If the date has passed, assume next year
                if parsed_date < datetime.now():
                    parsed_date = parsed_date.replace(year=current_year + 1)
            # Handle 2-digit years
            elif parsed_date.year < 100:
                current_year = datetime.now().year
                century = (current_year // 100) * 100
                parsed_year = century + parsed_date.year
                if parsed_year > current_year + 10:
                    parsed_year -= 100
                parsed_date = parsed_date.replace(year=parsed_year)
            return parsed_date.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    # Try regex extraction as fallback
    date_pattern = r'(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})'
    match = re.search(date_pattern, date_str)
    if match:
        month, day, year = match.groups()
        if len(year) == 2:
            year = f"20{year}" if int(year) < 50 else f"19{year}"
        try:
            return f"{year}-{int(month):02d}-{int(day):02d}"
        except ValueError:
            pass
    
    # Return original if parsing fails
    return date_str


def clean_name(name_str):
    """Clean tournament names."""
    if pd.isna(name_str) or str(name_str).strip() == '':
        return None
    
    name_str = str(name_str).strip()
    
    # Remove common suffixes/prefixes
    name_str = re.sub(r'\s?\*FULL\*$', '', name_str, flags=re.I)
    name_str = re.sub(r'\s?\(FULL\)$', '', name_str, flags=re.I)
    name_str = re.sub(r'\s?\[FULL\]$', '', name_str, flags=re.I)
    name_str = re.sub(r'^(?:View\s)?(?:Leaderboard|Results|Details|Info|Tee Times|Register|Enter)\s*[-–]?\s*', '', name_str, flags=re.I)
    name_str = re.sub(r'\s*[-–]?\s*(?:Leaderboard|Results|Details|Info|Tee Times|Register|Enter)$', '', name_str, flags=re.I)
    
    # Remove extra whitespace
    name_str = ' '.join(name_str.split())
    
    return name_str.strip() if name_str.strip() else None


def clean_course(course_str):
    """Clean golf course names."""
    if pd.isna(course_str) or str(course_str).strip() == '':
        return None
    
    course_str = str(course_str).strip()
    
    # Remove extra whitespace
    course_str = ' '.join(course_str.split())
    
    # Fix common abbreviations
    course_str = re.sub(r'\bGc\b', 'GC', course_str)
    course_str = re.sub(r'\bCc\b', 'CC', course_str)
    course_str = re.sub(r'\bG\.c\.\b', 'GC', course_str, flags=re.I)
    course_str = re.sub(r'\bC\.c\.\b', 'CC', course_str, flags=re.I)
    
    return course_str.strip() if course_str.strip() else None


def extract_category(row):
    """Extract tournament category (Senior, Amateur, Junior, All) from name or dedicated column."""
    # Get text to analyze
    name = str(row.get('name', '')).lower() if pd.notna(row.get('name')) else ""
    
    # Check if category column exists and has a value
    if 'category' in row.index and pd.notna(row.get('category')) and str(row.get('category')).strip():
        cat = str(row['category']).strip().lower()
        name = name + " " + cat  # Combine for analysis
    
    # Determine category based on keywords
    # Check for Super-Senior first (subset of Senior)
    if re.search(r'\bsuper.?senior\b', name):
        return 'Super-Senior'
    elif re.search(r'\bsenior\b|\bsr\.?\b', name):
        return 'Senior'
    elif re.search(r'\bjunior\b|\bjr\.?\b|\byouth\b|\bboys\b|\bgirls\b', name):
        return 'Junior'
    elif re.search(r'\bamateur\b|\bam\b', name):
        return 'Amateur'
    elif re.search(r'\bopen\b|\bchampionship\b|\bfour.?ball\b|\bmatch.?play\b|\bmid.?amateur\b|\bparent.?child\b', name):
        return 'Open'
    
    return 'All'  # Default to 'All' if no specific category detected


def extract_gender(row):
    """Extract gender (Men's, Women's) from name or dedicated column."""
    # Get text to analyze
    name = str(row.get('name', '')).lower() if pd.notna(row.get('name')) else ""
    
    # Check if category/gender column exists and has a value
    if 'category' in row.index and pd.notna(row.get('category')) and str(row.get('category')).strip():
        cat = str(row['category']).strip().lower()
        name = name + " " + cat
    if 'gender' in row.index and pd.notna(row.get('gender')) and str(row.get('gender')).strip():
        gender = str(row['gender']).strip().lower()
        name = name + " " + gender
    
    # Determine gender based on keywords
    if re.search(r'\bwomen\'?s\b|\bladies\b|\bfemale\b|\bgirls\b|\blpga\b', name):
        return "Women's"
    elif re.search(r'\bmen\'?s\b|\bmale\b|\bboys\b', name):
        return "Men's"
    elif re.search(r'\bparent.?child\b|\bfamily\b|\bmixed\b', name):
        return "Mixed"
    
    return "Men's"  # Default to Men's if no gender detected


def clean_city(city_str):
    """Clean city names."""
    if pd.isna(city_str) or str(city_str).strip() == '':
        return None
    
    city_str = str(city_str).strip()
    
    # Remove state/zip if accidentally included with city
    city_str = re.sub(r',\s*[A-Z]{2}\s*\d{5}(?:-\d{4})?$', '', city_str)
    city_str = re.sub(r',\s*[A-Z]{2}$', '', city_str)
    city_str = re.sub(r'\s+\d{5}(?:-\d{4})?$', '', city_str)
    
    # Remove extra whitespace
    city_str = ' '.join(city_str.split())
    
    # Title case
    city_str = city_str.title()
    
    # Fix common abbreviations
    city_str = re.sub(r'\bSt\.\s', 'Saint ', city_str)
    city_str = re.sub(r'\bFt\.\s', 'Fort ', city_str)
    city_str = re.sub(r'\bMt\.\s', 'Mount ', city_str)
    
    return city_str.strip() if city_str.strip() else None


def clean_state(state_str):
    """Clean and standardize state abbreviations."""
    if pd.isna(state_str) or str(state_str).strip() == '':
        return None
    
    state_str = str(state_str).strip().upper()
    
    # If already a valid 2-letter state code, return it
    valid_states = {
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC'
    }
    
    if state_str in valid_states:
        return state_str
    
    # Map of state names to abbreviations
    state_map = {
        'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR',
        'CALIFORNIA': 'CA', 'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE',
        'FLORIDA': 'FL', 'GEORGIA': 'GA', 'HAWAII': 'HI', 'IDAHO': 'ID',
        'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA', 'KANSAS': 'KS',
        'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD',
        'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS',
        'MISSOURI': 'MO', 'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV',
        'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ', 'NEW MEXICO': 'NM', 'NEW YORK': 'NY',
        'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', 'OHIO': 'OH', 'OKLAHOMA': 'OK',
        'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
        'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT',
        'VERMONT': 'VT', 'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV',
        'WISCONSIN': 'WI', 'WYOMING': 'WY', 'DISTRICT OF COLUMBIA': 'DC'
    }
    
    # Try to match full state name
    if state_str in state_map:
        return state_map[state_str]
    
    # Try partial match
    for full_name, abbr in state_map.items():
        if state_str in full_name or full_name.startswith(state_str):
            return abbr
    
    return state_str if len(state_str) == 2 else None


def clean_zip(zip_str):
    """Clean ZIP codes to 5-digit format."""
    if pd.isna(zip_str) or str(zip_str).strip() == '':
        return None
    
    zip_str = str(zip_str).strip()
    
    # Handle float conversion (e.g., 12345.0)
    if '.' in zip_str:
        zip_str = zip_str.split('.')[0]
    
    # Extract 5-digit ZIP code
    zip_match = re.search(r'(\d{5})(?:-\d{4})?', zip_str)
    if zip_match:
        return zip_match.group(1)
    
    # If it's just digits but less than 5, pad with zeros
    if zip_str.isdigit() and len(zip_str) < 5:
        return zip_str.zfill(5)
    
    return None


def find_column_match(target, columns):
    """Find the best matching column name."""
    target_lower = target.lower()
    columns_lower = {col: col.lower() for col in columns}
    
    # Exact match
    for col, col_lower in columns_lower.items():
        if col_lower == target_lower:
            return col
    
    # Contains match
    for col, col_lower in columns_lower.items():
        if target_lower in col_lower or col_lower in target_lower:
            return col
    
    # Common aliases
    aliases = {
        'date': ['date', 'tournament_date', 'event_date', 'start_date', 'dates'],
        'name': ['name', 'tournament_name', 'event_name', 'tournament', 'event', 'title'],
        'course': ['course', 'golf_course', 'venue', 'location', 'club', 'facility'],
        'category': ['category', 'type', 'division', 'class', 'flight', 'tournament_type'],
        'city': ['city', 'town', 'municipality'],
        'state': ['state', 'st', 'province', 'region'],
        'zip': ['zip', 'zipcode', 'zip_code', 'postal', 'postal_code']
    }
    
    if target_lower in aliases:
        for alias in aliases[target_lower]:
            for col, col_lower in columns_lower.items():
                if alias in col_lower:
                    return col
    
    return None


def clean_tournament_data(df):
    """Apply all cleaning operations to the dataframe."""
    cleaned_df = df.copy()
    
    # Normalize column names (lowercase, strip spaces)
    cleaned_df.columns = [str(col).strip().lower().replace(' ', '_') for col in cleaned_df.columns]
    
    # Map columns to expected names
    column_mapping = {}
    expected_columns = ['date', 'name', 'course', 'category', 'gender', 'city', 'state', 'zip']
    
    for expected in expected_columns:
        match = find_column_match(expected, cleaned_df.columns)
        if match and match != expected:
            column_mapping[match] = expected
    
    if column_mapping:
        cleaned_df = cleaned_df.rename(columns=column_mapping)
    
    # Ensure all expected columns exist
    for col in expected_columns:
        if col not in cleaned_df.columns:
            cleaned_df[col] = None
    
    # Apply cleaning functions
    if 'date' in cleaned_df.columns:
        cleaned_df['date'] = cleaned_df['date'].apply(clean_date)
    
    if 'name' in cleaned_df.columns:
        cleaned_df['name'] = cleaned_df['name'].apply(clean_name)
    
    if 'course' in cleaned_df.columns:
        cleaned_df['course'] = cleaned_df['course'].apply(clean_course)
    
    if 'city' in cleaned_df.columns:
        cleaned_df['city'] = cleaned_df['city'].apply(clean_city)
    
    if 'state' in cleaned_df.columns:
        cleaned_df['state'] = cleaned_df['state'].apply(clean_state)
    
    if 'zip' in cleaned_df.columns:
        cleaned_df['zip'] = cleaned_df['zip'].apply(clean_zip)
    
    # Extract category (Senior, Amateur, Junior, Open, All)
    cleaned_df['category'] = cleaned_df.apply(extract_category, axis=1)
    
    # Extract gender (Men's, Women's, Mixed)
    cleaned_df['gender'] = cleaned_df.apply(extract_gender, axis=1)
    
    # Reorder columns
    final_columns = ['date', 'name', 'course', 'category', 'gender', 'city', 'state', 'zip']
    other_columns = [col for col in cleaned_df.columns if col not in final_columns]
    cleaned_df = cleaned_df[final_columns + other_columns]
    
    # Capitalize column names for display
    cleaned_df.columns = [col.replace('_', ' ').title() for col in cleaned_df.columns]
    
    return cleaned_df


def extract_category_from_url(url):
    """Extract tournament category (Senior, Amateur, Junior, Open, All) from URL patterns."""
    if not url or pd.isna(url):
        return None
    
    url_lower = str(url).lower()
    
    # Check for category indicators in URL
    if any(keyword in url_lower for keyword in ['super-senior', 'supersenior']):
        return 'Super-Senior'
    elif any(keyword in url_lower for keyword in ['senior', 'seniors', 'sr-']):
        return 'Senior'
    elif any(keyword in url_lower for keyword in ['junior', 'juniors', 'jr-', 'youth', 'boys', 'girls']):
        return 'Junior'
    elif any(keyword in url_lower for keyword in ['amateur', 'am-']):
        return 'Amateur'
    elif any(keyword in url_lower for keyword in ['open', 'championship']):
        return 'Open'
    
    return None


def extract_gender_from_url(url):
    """Extract gender (Men's, Women's, Mixed) from URL patterns."""
    if not url or pd.isna(url):
        return None
    
    url_lower = str(url).lower()
    
    # Check for gender indicators in URL
    if any(keyword in url_lower for keyword in ['women', 'ladies', 'female', 'lpga', 'womens', 'girls']):
        return "Women's"
    elif any(keyword in url_lower for keyword in ['men', 'mens', 'male', 'boys']):
        return "Men's"
    elif any(keyword in url_lower for keyword in ['mixed', 'parent-child', 'family']):
        return "Mixed"
    
    return None


def extract_state_from_url(url):
    """Extract state from URL patterns."""
    if not url or pd.isna(url):
        return None
    
    url_lower = str(url).lower()
    
    # Map of URL patterns to state abbreviations
    # Priority order: specific domain patterns first, then general patterns
    state_patterns = [
        # Specific state golf association domains (check these first)
        ('fsga.org', 'FL'),      # Florida State Golf Association
        ('txga.org', 'TX'),      # Texas Golf Association
        ('tga.org', 'TX'),       # Texas Golf Association (alternate)
        ('gsga.org', 'GA'),      # Georgia State Golf Association
        ('scga.org', 'CA'),      # Southern California Golf Association
        ('ncga.org', 'CA'),      # Northern California Golf Association
        ('azga.org', 'AZ'),      # Arizona Golf Association
        ('aga.org', 'AZ'),       # Arizona Golf Association (alternate)
        ('cga.org', 'CO'),       # Colorado Golf Association
        ('wsga.org', 'WA'),      # Washington State Golf Association
        ('oga.org', 'OR'),       # Oregon Golf Association
        ('mga.org', 'MN'),       # Minnesota Golf Association
        ('gam.org', 'MI'),       # Golf Association of Michigan
        ('cdga.org', 'IL'),      # Chicago District Golf Association
        ('iga.org', 'IL'),       # Illinois Golf Association
        ('njsga.org', 'NJ'),     # New Jersey State Golf Association
        ('nysga.org', 'NY'),     # New York State Golf Association
        ('mga.org', 'NY'),       # Metropolitan Golf Association (NY area)
        ('vsga.org', 'VA'),      # Virginia State Golf Association
        ('carolinasgolf.org', 'NC'),  # Carolinas Golf Association
        ('tennessegolf.org', 'TN'),   # Tennessee Golf Association
        ('ohiogolf.org', 'OH'),       # Ohio Golf Association
        ('indianagolf.org', 'IN'),    # Indiana Golf Association
        ('kygolf.org', 'KY'),         # Kentucky Golf Association
        ('missourigolf.org', 'MO'),   # Missouri Golf Association
        ('iowagolf.org', 'IA'),       # Iowa Golf Association
        ('nebgolf.org', 'NE'),        # Nebraska Golf Association
        ('kansasgolf.org', 'KS'),     # Kansas Golf Association
        ('okgolf.org', 'OK'),         # Oklahoma Golf Association
        ('arkansasgolf.org', 'AR'),   # Arkansas Golf Association
        ('msgolf.org', 'MS'),         # Mississippi Golf Association
        ('algolf.org', 'AL'),         # Alabama Golf Association
        ('lgagolf.org', 'LA'),        # Louisiana Golf Association
        ('snga.org', 'NV'),           # Southern Nevada Golf Association
        ('utahgolf.org', 'UT'),       # Utah Golf Association
        ('nmga.org', 'NM'),           # New Mexico Golf Association
        ('hsgagolf.org', 'HI'),       # Hawaii State Golf Association
        ('agagolf.org', 'AK'),        # Alaska Golf Association
        ('megagolf.org', 'ME'),       # Maine Golf Association
        ('vtga.org', 'VT'),           # Vermont Golf Association
        ('nhga.org', 'NH'),           # New Hampshire Golf Association
        ('riga.org', 'RI'),           # Rhode Island Golf Association
        ('dsga.org', 'DE'),           # Delaware State Golf Association
        ('wvga.org', 'WV'),           # West Virginia Golf Association
        ('msgagolf.org', 'MT'),       # Montana State Golf Association
        ('theiga.org', 'ID'),         # Idaho Golf Association
        ('wga.org', 'WY'),            # Wyoming Golf Association
        ('ndga.org', 'ND'),           # North Dakota Golf Association
        ('sdga.org', 'SD'),           # South Dakota Golf Association
        ('wiscgolf.org', 'WI'),       # Wisconsin State Golf Association
        ('wpga.org', 'WI'),           # Wisconsin PGA
        ('massgolf.org', 'MA'),       # Massachusetts Golf Association
        ('ctga.org', 'CT'),           # Connecticut Golf Association
        ('mdga.org', 'MD'),           # Maryland State Golf Association
        ('pagolf.org', 'PA'),         # Pennsylvania Golf Association
        
        # State name patterns in URL
        ('florida', 'FL'),
        ('texas', 'TX'),
        ('georgia', 'GA'),
        ('california', 'CA'),
        ('arizona', 'AZ'),
        ('colorado', 'CO'),
        ('newyork', 'NY'),
        ('new-york', 'NY'),
        ('newjersey', 'NJ'),
        ('new-jersey', 'NJ'),
        ('northcarolina', 'NC'),
        ('north-carolina', 'NC'),
        ('southcarolina', 'SC'),
        ('south-carolina', 'SC'),
        ('virginia', 'VA'),
        ('ohio', 'OH'),
        ('michigan', 'MI'),
        ('illinois', 'IL'),
        ('pennsylvania', 'PA'),
        ('massachusetts', 'MA'),
        ('washington', 'WA'),
        ('oregon', 'OR'),
        ('nevada', 'NV'),
        ('tennessee', 'TN'),
        ('alabama', 'AL'),
        ('louisiana', 'LA'),
        ('minnesota', 'MN'),
        ('wisconsin', 'WI'),
        ('iowa', 'IA'),
        ('missouri', 'MO'),
        ('kansas', 'KS'),
        ('oklahoma', 'OK'),
        ('arkansas', 'AR'),
        ('mississippi', 'MS'),
        ('kentucky', 'KY'),
        ('indiana', 'IN'),
        ('maryland', 'MD'),
        ('connecticut', 'CT'),
        ('utah', 'UT'),
        ('newmexico', 'NM'),
        ('new-mexico', 'NM'),
        ('hawaii', 'HI'),
        ('alaska', 'AK'),
        ('maine', 'ME'),
        ('vermont', 'VT'),
        ('newhampshire', 'NH'),
        ('new-hampshire', 'NH'),
        ('rhodeisland', 'RI'),
        ('rhode-island', 'RI'),
        ('delaware', 'DE'),
        ('westvirginia', 'WV'),
        ('west-virginia', 'WV'),
        ('montana', 'MT'),
        ('idaho', 'ID'),
        ('wyoming', 'WY'),
        ('northdakota', 'ND'),
        ('north-dakota', 'ND'),
        ('southdakota', 'SD'),
        ('south-dakota', 'SD'),
        ('nebraska', 'NE'),
    ]
    
    for pattern, state in state_patterns:
        if pattern in url_lower:
            return state
    
    return None


def filter_old_dates(df, raw_text_content=None):
    """Filter out tournaments with entries_close_year of 2025 or earlier."""
    if df is None or len(df) == 0:
        return df
    
    df = df.copy()
    rows_to_keep = []
    
    # Find the entries_close_year column (case insensitive)
    ec_year_col = None
    for col in df.columns:
        if 'entries_close_year' in col.lower().replace(' ', '_'):
            ec_year_col = col
            break
    
    for idx, row in df.iterrows():
        # Check entries_close_year column first (most reliable)
        if ec_year_col:
            ec_year = row.get(ec_year_col)
            if pd.notna(ec_year):
                try:
                    year = int(ec_year)
                    if year <= 2025:
                        # Skip - entries close is in 2025 or earlier
                        continue
                    else:
                        rows_to_keep.append(idx)
                        continue
                except (ValueError, TypeError):
                    pass
        
        # Fallback: check all text in the row for years
        all_text = ' '.join(str(v) for v in row.values if pd.notna(v))
        all_years = re.findall(r'\b(20\d{2})\b', all_text)
        
        if all_years:
            years = [int(y) for y in all_years]
            # If any year is 2025 or earlier, skip
            if min(years) <= 2025:
                continue
            else:
                rows_to_keep.append(idx)
        else:
            # No year found anywhere - keep the row
            rows_to_keep.append(idx)
    
    # Return filtered dataframe (drop the entries_close_year column from output)
    result_df = df.loc[rows_to_keep].reset_index(drop=True)
    if ec_year_col and ec_year_col in result_df.columns:
        result_df = result_df.drop(columns=[ec_year_col])
    
    return result_df


def apply_url_based_defaults(df, source_url=None):
    """Apply category and state defaults based on URL when values are missing."""
    if df is None or len(df) == 0:
        return df
    
    df = df.copy()
    
    # Normalize column names for checking
    col_map = {col.lower().replace(' ', '_'): col for col in df.columns}
    
    category_col = col_map.get('category')
    state_col = col_map.get('state')
    source_col = col_map.get('source_url')
    
    # Determine the URL to use for inference
    def get_url_for_row(row):
        if source_col and pd.notna(row.get(source_col)):
            return row[source_col]
        return source_url
    
    # Get gender column
    gender_col = col_map.get('gender')
    
    # Apply category defaults
    if category_col:
        for idx, row in df.iterrows():
            current_category = row.get(category_col)
            if pd.isna(current_category) or str(current_category).strip() == '' or current_category == 'All':
                url = get_url_for_row(row)
                # Try to get category from URL
                url_category = extract_category_from_url(url)
                if url_category:
                    df.at[idx, category_col] = url_category
                elif pd.isna(current_category) or str(current_category).strip() == '':
                    # Default to 'All' if no category can be determined
                    df.at[idx, category_col] = "All"
    
    # Apply gender defaults
    if gender_col:
        for idx, row in df.iterrows():
            current_gender = row.get(gender_col)
            if pd.isna(current_gender) or str(current_gender).strip() == '':
                url = get_url_for_row(row)
                # Try to get gender from URL
                url_gender = extract_gender_from_url(url)
                if url_gender:
                    df.at[idx, gender_col] = url_gender
                else:
                    # Default to Men's if no gender can be determined
                    df.at[idx, gender_col] = "Men's"
    
    # Apply state defaults
    if state_col:
        for idx, row in df.iterrows():
            current_state = row.get(state_col)
            if pd.isna(current_state) or str(current_state).strip() == '':
                url = get_url_for_row(row)
                url_state = extract_state_from_url(url)
                if url_state:
                    df.at[idx, state_col] = url_state
    
    return df


def get_csv_download_link(df, filename="cleaned_tournament_data.csv"):
    """Generate a download link for CSV file."""
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    return f'<a href="data:file/csv;base64,{b64}" download="{filename}" class="download-btn">📥 Download CSV</a>'


def get_excel_download_link(df, filename="cleaned_tournament_data.xlsx"):
    """Generate a download link for Excel file."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Tournaments')
    excel_data = output.getvalue()
    b64 = base64.b64encode(excel_data).decode()
    return f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{filename}" class="download-btn">📥 Download Excel</a>'


# --- URL Scraping with AI ---

def fetch_page_content(url):
    """Fetch HTML content from a URL."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        st.error(f"Error fetching URL: {str(e)}")
        return None


def extract_text_from_html(html_content):
    """Extract readable text from HTML, focusing on structured tournament data."""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove script and style elements
    for element in soup(['script', 'style', 'nav', 'footer', 'header']):
        element.decompose()
    
    extracted_data = []
    
    # Get the page title for context
    title = soup.title.string if soup.title else ""
    extracted_data.append(f"Page Title: {title}\n")
    
    # 1. Try to find tables first (most tournament data is in tables)
    tables = soup.find_all('table')
    if tables:
        extracted_data.append("\n=== TABLE DATA ===")
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                row_text = ' | '.join(cell.get_text(strip=True) for cell in cells)
                if row_text.strip():
                    extracted_data.append(row_text)
    
    # 2. Look for FSGA-style striped rows (div-based layouts)
    striped_containers = soup.find_all('div', class_=lambda x: x and 'striped' in str(x).lower())
    if striped_containers:
        extracted_data.append("\n=== STRIPED ROW DATA ===")
        for container in striped_containers:
            rows = container.find_all('div', class_='row', recursive=False)
            for row in rows:
                # Get all column divs
                cols = row.find_all('div', recursive=False)
                row_parts = []
                for col in cols:
                    text = col.get_text(strip=True)
                    if text:
                        row_parts.append(text)
                if row_parts:
                    extracted_data.append(' | '.join(row_parts))
    
    # 3. Look for card-based layouts
    cards = soup.find_all(['div', 'article'], class_=lambda x: x and any(
        keyword in str(x).lower() for keyword in ['card', 'event-item', 'tournament-item', 'list-item', 'schedule-item']
    ))
    if cards:
        extracted_data.append("\n=== CARD DATA ===")
        for card in cards[:100]:  # Limit to avoid too much data
            text = card.get_text(separator=' | ', strip=True)
            if len(text) > 15 and len(text) < 1000:
                extracted_data.append(text)
    
    # 4. Look for list-based layouts
    list_containers = soup.find_all(['ul', 'ol'], class_=lambda x: x and any(
        keyword in str(x).lower() for keyword in ['tournament', 'event', 'schedule', 'list']
    ))
    if list_containers:
        extracted_data.append("\n=== LIST DATA ===")
        for container in list_containers:
            items = container.find_all('li')
            for item in items:
                text = item.get_text(strip=True)
                if len(text) > 15:
                    extracted_data.append(text)
    
    # 5. Look for generic row-based layouts (Bootstrap-style)
    if len(extracted_data) < 5:  # If we haven't found much structured data
        row_divs = soup.find_all('div', class_=lambda x: x and 'row' in str(x).lower())
        seen_texts = set()
        extracted_data.append("\n=== ROW DATA ===")
        for row in row_divs:
            # Skip if this is a navigation or header row
            if row.find_parent(['nav', 'header', 'footer']):
                continue
            text = row.get_text(separator=' | ', strip=True)
            # Only include rows that look like tournament data (have dates or golf keywords)
            if len(text) > 30 and len(text) < 500:
                if any(keyword in text.lower() for keyword in ['golf', 'club', 'course', 'championship', 'open', 'amateur', 'enter', 'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']):
                    if text not in seen_texts:
                        seen_texts.add(text)
                        extracted_data.append(text)
    
    # 6. If still no data, get main content
    if len(extracted_data) < 5:
        main_content = soup.find('main') or soup.find('body')
        if main_content:
            extracted_data.append("\n=== MAIN CONTENT ===")
            extracted_data.append(main_content.get_text(separator='\n', strip=True))
    
    combined_text = '\n'.join(extracted_data)
    
    # Limit text length to avoid token limits (increased to capture more data)
    return combined_text[:30000]


def parse_tournaments_with_ai(text_content, api_key, chunk_size=12000):
    """Use OpenAI to parse tournament data from text. Handles large content by chunking."""
    
    client = OpenAI(api_key=api_key)
    
    # If content is very large, process in chunks
    if len(text_content) > chunk_size:
        all_tournaments = []
        chunks = []
        
        # Split by lines to avoid cutting mid-tournament
        lines = text_content.split('\n')
        current_chunk = ""
        
        for line in lines:
            if len(current_chunk) + len(line) > chunk_size:
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = line
            else:
                current_chunk += "\n" + line if current_chunk else line
        
        if current_chunk:
            chunks.append(current_chunk)
        
        st.info(f"Processing {len(chunks)} chunks of content...")
        
        for i, chunk in enumerate(chunks):
            st.text(f"Processing chunk {i+1}/{len(chunks)}...")
            chunk_tournaments = _parse_single_chunk(client, chunk)
            if chunk_tournaments:
                all_tournaments.extend(chunk_tournaments)
        
        return all_tournaments
    else:
        return _parse_single_chunk(client, text_content)


def _parse_single_chunk(client, text_content):
    """Parse a single chunk of text content with AI."""
    
    prompt = """You are a data extraction expert. Extract ALL golf tournament information from the following webpage content.

IMPORTANT: Extract EVERY tournament entry you find. Do not skip any. Each row/entry typically contains:
- A date (like "Jan 13", "Feb 7-8", "Mar 2", etc.)
- An "Entries Close" date with a year (like "Entries Close: August 13, 2025")
- A tournament/event name (may include "*FULL*" marker)
- A golf course or venue name
- Location info (city, state like "FL" or "Florida")

For each tournament found, extract:
- date: The tournament date exactly as shown (e.g., "Jan 13", "Feb 7-8")
- entries_close_year: The YEAR from the "Entries Close" date if present (e.g., 2025, 2026). This is CRITICAL for filtering. Look for patterns like "Entries Close: August 13, 2025" and extract just the year (2025).
- name: The tournament/event name (remove *FULL* markers)
- course: The golf course or venue name (e.g., "Southern Hills Plantation Club", "Black Diamond Ranch")
- category: The category if mentioned (Senior, Men's, Women's, Junior, Four-Ball, etc.) - look for keywords in the name
- city: The city (e.g., "Brooksville", "Fort Myers")
- state: The state abbreviation (e.g., "FL")
- zip: The ZIP code if available (usually null)

Return the data as a JSON array of objects. If a field is not found, use null.
Extract ALL tournaments - there may be 10, 20, or more entries.
Skip navigation items, headers, footers, and non-tournament content.

Example output format:
[
  {"date": "Jan 13", "entries_close_year": 2026, "name": "Senior Championship", "course": "Southern Hills Plantation Club", "category": "Senior", "city": "Brooksville", "state": "FL", "zip": null},
  {"date": "May 2-6", "entries_close_year": 2025, "name": "U.S. Women's Amateur Four-Ball", "course": "Desert Mountain Club", "category": "Women's", "city": "Scottsdale", "state": "AZ", "zip": null}
]

Webpage content:
"""
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a precise data extraction assistant. Extract ALL tournament entries - do not skip any. Always respond with valid JSON only, no markdown formatting or explanation."},
                {"role": "user", "content": prompt + text_content}
            ],
            temperature=0.1,
            max_tokens=16000  # Increased to handle more tournaments
        )
        
        result = response.choices[0].message.content.strip()
        
        # Clean up the response - remove markdown code blocks if present
        if result.startswith('```'):
            result = re.sub(r'^```(?:json)?\n?', '', result)
            result = re.sub(r'\n?```$', '', result)
        
        # Try to parse JSON
        try:
            tournaments = json.loads(result)
            return tournaments
        except json.JSONDecodeError as e:
            # Try to fix truncated JSON by finding the last complete object
            st.warning("Response was truncated. Attempting to recover partial data...")
            
            # Try to find the last complete JSON object
            # Look for the last "}," or "}" that completes an object
            last_complete = result.rfind('},')
            if last_complete > 0:
                # Try to close the array after the last complete object
                fixed_result = result[:last_complete + 1] + ']'
                try:
                    tournaments = json.loads(fixed_result)
                    st.info(f"Recovered {len(tournaments)} tournaments from truncated response")
                    return tournaments
                except json.JSONDecodeError:
                    pass
            
            # Try another approach - find last complete object ending with }
            last_brace = result.rfind('}')
            if last_brace > 0:
                # Count brackets to find valid JSON
                test_result = result[:last_brace + 1]
                # Make sure it ends properly
                if not test_result.rstrip().endswith(']'):
                    test_result = test_result + ']'
                try:
                    tournaments = json.loads(test_result)
                    st.info(f"Recovered {len(tournaments)} tournaments from truncated response")
                    return tournaments
                except json.JSONDecodeError:
                    pass
            
            # If all recovery attempts fail, show error
            st.error(f"Error parsing AI response: {str(e)}")
            with st.expander("Show raw response (for debugging)"):
                st.code(result[:2000] + "..." if len(result) > 2000 else result)
            return []
        
    except Exception as e:
        st.error(f"Error calling OpenAI API: {str(e)}")
        return []


def process_url_with_ai(url, api_key):
    """Full pipeline: fetch URL, extract text, parse with AI, clean data."""
    
    # Step 1: Fetch the page
    with st.spinner("Fetching webpage..."):
        html_content = fetch_page_content(url)
        if not html_content:
            return None
    
    # Step 2: Extract text
    with st.spinner("Extracting content..."):
        text_content = extract_text_from_html(html_content)
        if not text_content or len(text_content) < 100:
            st.warning("Could not extract meaningful content from the page. The page might require JavaScript to load.")
            return None
    
    # Step 3: Parse with AI
    with st.spinner("AI is analyzing the content..."):
        tournaments = parse_tournaments_with_ai(text_content, api_key)
        if not tournaments:
            st.warning("No tournaments found on this page.")
            return None
    
    # Step 4: Convert to DataFrame and clean
    df = pd.DataFrame(tournaments)
    cleaned_df = clean_tournament_data(df)
    
    # Step 5: Apply URL-based defaults for missing Category and State
    cleaned_df = apply_url_based_defaults(cleaned_df, source_url=url)
    
    # Step 6: Filter out tournaments with dates from 2025 or earlier
    # Pass the raw text content so we can check "Entries Close" dates
    cleaned_df = filter_old_dates(cleaned_df, raw_text_content=text_content)
    
    return cleaned_df


# --- Streamlit UI ---
def main():
    # Custom CSS
    st.markdown("""
        <style>
        .main-header {
            font-size: 2.5rem;
            font-weight: 700;
            color: #1e5631;
            margin-bottom: 0.5rem;
        }
        .sub-header {
            font-size: 1.1rem;
            color: #666;
            margin-bottom: 2rem;
        }
        .download-btn {
            display: inline-block;
            padding: 0.5rem 1rem;
            background-color: #1e5631;
            color: white !important;
            text-decoration: none;
            border-radius: 5px;
            margin: 0.25rem;
            font-weight: 500;
        }
        .download-btn:hover {
            background-color: #2d7a47;
        }
        .stats-box {
            background-color: #f0f7f0;
            padding: 1rem;
            border-radius: 10px;
            border-left: 4px solid #1e5631;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 24px;
        }
        .stTabs [data-baseweb="tab"] {
            height: 50px;
            padding: 10px 20px;
            font-weight: 600;
        }
        </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<p class="main-header">⛳ Golf Tournament Data Extractor</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Extract and clean tournament data from URLs or CSV files.</p>', unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Settings")
        
        # API Key input
        api_key = st.text_input(
            "OpenAI API Key",
            type="password",
            help="Required for URL parsing. Get your key at platform.openai.com"
        )
        
        if api_key:
            st.success("✓ API key provided")
        else:
            st.info("Enter API key to enable URL parsing")
        
        st.divider()
        
        st.header("📋 Instructions")
        st.markdown("""
        **Option 1: URL Parsing (AI)**
        - Enter one or more URLs (one per line)
        - AI extracts tournament data automatically
        
        **Option 2: CSV Upload**
        - Upload your own CSV file
        - Data gets cleaned and standardized
        
        **Option 3: Paste Content**
        - Copy text from protected pages
        - AI extracts tournament data
        """)
        
        st.divider()
        
        # --- Combined Results Section ---
        col_header, col_refresh = st.columns([3, 1])
        with col_header:
            st.header("📦 Combined Results")
        with col_refresh:
            st.write("")  # Spacing
            if st.button("🔄", help="Refresh to see latest count", key="refresh_sidebar"):
                st.rerun()
        
        # Initialize combined results in session state
        if 'combined_results' not in st.session_state:
            st.session_state['combined_results'] = pd.DataFrame()
        
        combined_df = st.session_state['combined_results']
        
        if len(combined_df) > 0:
            st.success(f"**{len(combined_df)}** tournaments collected")
            
            # Show breakdown by source if available
            if 'Source URL' in combined_df.columns or 'Source' in combined_df.columns:
                source_col = 'Source URL' if 'Source URL' in combined_df.columns else 'Source'
                sources = combined_df[source_col].nunique()
                st.caption(f"From {sources} source(s)")
            
            # Download buttons
            st.markdown(get_csv_download_link(combined_df, "all_tournaments.csv"), unsafe_allow_html=True)
            st.markdown(get_excel_download_link(combined_df, "all_tournaments.xlsx"), unsafe_allow_html=True)
            
            # Clear button
            if st.button("🗑️ Clear All", use_container_width=True):
                st.session_state['combined_results'] = pd.DataFrame()
                st.rerun()
            
            # Preview expander
            with st.expander("Preview data"):
                st.dataframe(combined_df, height=200)
        else:
            st.info("Extract data from any tab to start collecting tournaments here.")
    
    # Main content with tabs
    tab1, tab2, tab3 = st.tabs(["🌐 Parse from URL(s)", "📄 Upload CSV", "📋 Paste Content"])
    
    # --- TAB 1: URL Parsing ---
    with tab1:
        st.markdown("### Enter tournament schedule URLs")
        st.markdown("*Enter one URL per line to extract data from multiple sources*")
        
        urls_input = st.text_area(
            "URLs",
            placeholder="https://www.fsga.org/TournamentCategory/EnterList/...\nhttps://wpga-onlineregistration.golfgenius.com/pages/...\nhttps://usamtour.bluegolf.com/bluegolf/...",
            height=120,
            label_visibility="collapsed"
        )
        
        col1, col2 = st.columns([1, 3])
        with col1:
            parse_button = st.button("🔍 Extract Data", type="primary", use_container_width=True)
        with col2:
            if 'url_results' in st.session_state and st.session_state['url_results'] is not None:
                clear_button = st.button("🗑️ Clear Results", use_container_width=False)
                if clear_button:
                    st.session_state['url_results'] = None
                    st.session_state['processed_urls'] = []
                    st.rerun()
        
        if parse_button:
            if not urls_input.strip():
                st.error("Please enter at least one URL")
            elif not api_key:
                st.error("Please enter your OpenAI API key in the sidebar")
            else:
                # Parse multiple URLs
                urls = [url.strip() for url in urls_input.strip().split('\n') if url.strip()]
                
                if len(urls) == 0:
                    st.error("Please enter at least one valid URL")
                else:
                    all_results = []
                    processed_urls = []
                    
                    # Progress tracking
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    for i, url in enumerate(urls):
                        status_text.text(f"Processing URL {i+1} of {len(urls)}: {url[:50]}...")
                        progress_bar.progress((i) / len(urls))
                        
                        try:
                            result_df = process_url_with_ai(url, api_key)
                            
                            if result_df is not None and len(result_df) > 0:
                                # Add source URL column
                                result_df['Source URL'] = url
                                all_results.append(result_df)
                                processed_urls.append({'url': url, 'count': len(result_df), 'status': '✅'})
                                st.success(f"✅ Found {len(result_df)} tournaments from {url[:50]}...")
                            else:
                                processed_urls.append({'url': url, 'count': 0, 'status': '⚠️'})
                                st.warning(f"⚠️ No tournaments found from {url[:50]}...")
                        except Exception as e:
                            processed_urls.append({'url': url, 'count': 0, 'status': '❌'})
                            st.error(f"❌ Error processing {url[:50]}...: {str(e)}")
                    
                    progress_bar.progress(1.0)
                    status_text.text("Processing complete!")
                    
                    # Combine all results
                    if all_results:
                        combined_df = pd.concat(all_results, ignore_index=True)
                        st.session_state['url_results'] = combined_df
                        st.session_state['processed_urls'] = processed_urls
                        
                        # Add to combined results in sidebar
                        if 'combined_results' not in st.session_state:
                            st.session_state['combined_results'] = pd.DataFrame()
                        st.session_state['combined_results'] = pd.concat(
                            [st.session_state['combined_results'], combined_df], 
                            ignore_index=True
                        ).drop_duplicates(subset=['Date', 'Name', 'Course'], keep='first')
                        
                        total_tournaments = len(combined_df)
                        st.success(f"🎉 Total: {total_tournaments} tournaments extracted from {len([p for p in processed_urls if p['status'] == '✅'])} URL(s)!")
                        st.info(f"📦 Added to combined results ({len(st.session_state['combined_results'])} total in sidebar)")
                    else:
                        st.error("No tournaments were extracted from any of the URLs.")
        
        # Display results
        if 'url_results' in st.session_state and st.session_state['url_results'] is not None:
            df = st.session_state['url_results']
            
            # Show processing summary if multiple URLs were processed
            if 'processed_urls' in st.session_state and len(st.session_state['processed_urls']) > 1:
                with st.expander("📋 Processing Summary", expanded=False):
                    summary_df = pd.DataFrame(st.session_state['processed_urls'])
                    summary_df.columns = ['URL', 'Tournaments Found', 'Status']
                    st.dataframe(summary_df, use_container_width=True)
            
            st.markdown("### 📊 Extracted Tournament Data")
            st.dataframe(df, use_container_width=True, height=400)
            
            # Stats
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Tournaments", len(df))
            with col2:
                valid_dates = df['Date'].notna().sum()
                st.metric("Valid Dates", f"{valid_dates}/{len(df)}")
            with col3:
                valid_courses = df['Course'].notna().sum()
                st.metric("Valid Courses", f"{valid_courses}/{len(df)}")
            with col4:
                if 'Source URL' in df.columns:
                    unique_sources = df['Source URL'].nunique()
                    st.metric("Sources", unique_sources)
                else:
                    categories = df['Category'].notna().sum()
                    st.metric("Categories Found", f"{categories}/{len(df)}")
            
            # Download buttons
            st.markdown("### 📥 Download")
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(get_csv_download_link(df, "tournament_data.csv"), unsafe_allow_html=True)
            with col2:
                st.markdown(get_excel_download_link(df, "tournament_data.xlsx"), unsafe_allow_html=True)
    
    # --- TAB 2: CSV Upload ---
    with tab2:
        st.markdown("### Upload a CSV file with tournament data")
        
        uploaded_file = st.file_uploader(
            "Choose a CSV file",
            type=['csv'],
            help="Upload a CSV file containing tournament data"
        )
        
        if uploaded_file is not None:
            try:
                # Read the CSV file
                df = pd.read_csv(uploaded_file)
                
                # Display original data
                st.markdown("### 📄 Original Data")
                st.dataframe(df, use_container_width=True, height=250)
                
                # Clean the data
                with st.spinner("Cleaning data..."):
                    cleaned_df = clean_tournament_data(df)
                
                # Display cleaned data
                st.markdown("### ✨ Cleaned Data")
                st.dataframe(cleaned_df, use_container_width=True, height=350)
                
                # Stats
                st.markdown("### 📊 Cleaning Summary")
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    valid_dates = cleaned_df['Date'].notna().sum()
                    st.metric("Valid Dates", f"{valid_dates}/{len(cleaned_df)}")
                
                with col2:
                    valid_names = cleaned_df['Name'].notna().sum()
                    st.metric("Valid Names", f"{valid_names}/{len(cleaned_df)}")
                
                with col3:
                    valid_states = cleaned_df['State'].notna().sum()
                    st.metric("Valid States", f"{valid_states}/{len(cleaned_df)}")
                
                with col4:
                    valid_categories = cleaned_df['Category'].notna().sum()
                    st.metric("Categories", f"{valid_categories}/{len(cleaned_df)}")
                
                # Add to combined results button
                st.markdown("### 📦 Add to Combined Results")
                if st.button("➕ Add to Combined Results", key="add_csv_to_combined", use_container_width=True):
                    cleaned_df_with_source = cleaned_df.copy()
                    cleaned_df_with_source['Source'] = uploaded_file.name
                    
                    if 'combined_results' not in st.session_state:
                        st.session_state['combined_results'] = pd.DataFrame()
                    st.session_state['combined_results'] = pd.concat(
                        [st.session_state['combined_results'], cleaned_df_with_source], 
                        ignore_index=True
                    ).drop_duplicates(subset=['Date', 'Name', 'Course'], keep='first')
                    st.success(f"✅ Added {len(cleaned_df)} tournaments to combined results ({len(st.session_state['combined_results'])} total)")
                
                # Download options
                st.markdown("### 📥 Download This File Only")
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown(get_csv_download_link(cleaned_df), unsafe_allow_html=True)
                with col2:
                    st.markdown(get_excel_download_link(cleaned_df), unsafe_allow_html=True)
                    
            except Exception as e:
                st.error(f"Error processing file: {str(e)}")
        
        else:
            st.info("👆 Upload a CSV file to get started")
            
            with st.expander("📋 See example CSV format"):
                sample_data = pd.DataFrame({
                    'Date': ['5/15/2025', 'June 3, 2025', '2025-07-20'],
                    'Name': ['Senior Championship *FULL*', 'Junior Open', "Women's Amateur Classic"],
                    'Course': ['Pine Valley GC', 'Augusta National Golf Club', 'Pebble Beach'],
                    'Category': ['', '', ''],
                    'City': ['Clementon', 'Augusta', 'Pebble Beach'],
                    'State': ['New Jersey', 'GA', 'California'],
                    'Zip': ['08021', '30904', '93953']
                })
                st.dataframe(sample_data, use_container_width=True)
    
    # --- TAB 3: Paste Content ---
    with tab3:
        st.markdown("### Paste content from a protected page")
        st.markdown("*For pages that require login (like Golf Genius), simply copy the visible text or HTML and paste it here.*")
        
        st.info("""
        **How to copy content:**
        - **Easy way:** Select all visible text on the page (Cmd+A / Ctrl+A) and copy (Cmd+C / Ctrl+C)
        - **Alternative:** Right-click → View Page Source, then copy all HTML
        
        Both formats work! The AI will extract tournament data from whatever you paste.
        """)
        
        # Optional: source URL for state/category inference
        source_url_input = st.text_input(
            "Source URL (optional)",
            placeholder="https://www.golfgenius.com/leagues/36562/...",
            help="Enter the original URL to help infer state and category"
        )
        
        html_input = st.text_area(
            "Paste content here",
            placeholder="Paste tournament listings here...\n\nExample:\n44th Alabama State Four-Ball Championship\nWed, Apr 29, 2026 - Sat, May 2, 2026\nCourse: Canebrake Club\n...",
            height=250,
            label_visibility="collapsed"
        )
        
        col1, col2 = st.columns([1, 3])
        with col1:
            parse_html_button = st.button("🔍 Extract Data", type="primary", use_container_width=True, key="parse_html")
        with col2:
            if 'html_results' in st.session_state and st.session_state['html_results'] is not None:
                clear_html_button = st.button("🗑️ Clear Results", use_container_width=False, key="clear_html")
                if clear_html_button:
                    st.session_state['html_results'] = None
                    st.rerun()
        
        if parse_html_button:
            if not html_input.strip():
                st.error("Please paste content")
            elif not api_key:
                st.error("Please enter your OpenAI API key in the sidebar")
            elif len(html_input) < 100:
                st.error("Content seems too short. Make sure you copied enough text.")
            else:
                try:
                    # Determine if it's HTML or plain text
                    is_html = html_input.strip().startswith('<') or '<html' in html_input.lower() or '<div' in html_input.lower()
                    
                    if is_html:
                        # Extract text from HTML
                        with st.spinner("Extracting content from HTML..."):
                            text_content = extract_text_from_html(html_input)
                    else:
                        # Use the text directly
                        text_content = html_input.strip()
                    
                    if not text_content or len(text_content) < 50:
                        st.warning("Could not extract meaningful content.")
                    else:
                        st.success(f"Processing {len(text_content)} characters of content...")
                        
                        # Parse with AI
                        with st.spinner("AI is analyzing the content..."):
                            tournaments = parse_tournaments_with_ai(text_content, api_key)
                            
                            if not tournaments:
                                st.warning("No tournaments found in the content.")
                            else:
                                # Convert to DataFrame and clean
                                df = pd.DataFrame(tournaments)
                                cleaned_df = clean_tournament_data(df)
                                
                                # Apply URL-based defaults if source URL provided
                                if source_url_input.strip():
                                    cleaned_df = apply_url_based_defaults(cleaned_df, source_url=source_url_input.strip())
                                else:
                                    cleaned_df = apply_url_based_defaults(cleaned_df)
                                
                                # Filter old dates
                                cleaned_df = filter_old_dates(cleaned_df, raw_text_content=text_content)
                                
                                # Add source info
                                if source_url_input.strip():
                                    cleaned_df['Source URL'] = source_url_input.strip()
                                else:
                                    cleaned_df['Source'] = 'Pasted Content'
                                
                                st.session_state['html_results'] = cleaned_df
                                
                                # Add to combined results
                                if 'combined_results' not in st.session_state:
                                    st.session_state['combined_results'] = pd.DataFrame()
                                st.session_state['combined_results'] = pd.concat(
                                    [st.session_state['combined_results'], cleaned_df], 
                                    ignore_index=True
                                ).drop_duplicates(subset=['Date', 'Name', 'Course'], keep='first')
                                
                                st.success(f"🎉 Found {len(cleaned_df)} tournaments!")
                                st.info(f"📦 Added to combined results ({len(st.session_state['combined_results'])} total in sidebar)")
                            
                except Exception as e:
                    st.error(f"Error processing content: {str(e)}")
        
        # Display results
        if 'html_results' in st.session_state and st.session_state['html_results'] is not None:
            df = st.session_state['html_results']
            
            st.markdown("### 📊 Extracted Tournament Data")
            st.dataframe(df, use_container_width=True, height=400)
            
            # Stats
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Tournaments", len(df))
            with col2:
                valid_dates = df['Date'].notna().sum()
                st.metric("Valid Dates", f"{valid_dates}/{len(df)}")
            with col3:
                valid_courses = df['Course'].notna().sum()
                st.metric("Valid Courses", f"{valid_courses}/{len(df)}")
            with col4:
                valid_states = df['State'].notna().sum()
                st.metric("Valid States", f"{valid_states}/{len(df)}")
            
            # Download buttons
            st.markdown("### 📥 Download")
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(get_csv_download_link(df, "tournament_data_from_html.csv"), unsafe_allow_html=True)
            with col2:
                st.markdown(get_excel_download_link(df, "tournament_data_from_html.xlsx"), unsafe_allow_html=True)


if __name__ == "__main__":
    main()
