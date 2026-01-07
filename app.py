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
    """Extract tournament category from name or dedicated column."""
    # Check if category column exists and has a value
    if 'category' in row.index and pd.notna(row.get('category')) and str(row.get('category')).strip():
        cat = str(row['category']).strip()
        # Standardize existing categories
        cat_lower = cat.lower()
        if 'senior' in cat_lower or 'sr' in cat_lower:
            return 'Senior'
        elif 'junior' in cat_lower or 'jr' in cat_lower or 'youth' in cat_lower:
            return 'Junior'
        elif 'women' in cat_lower or 'ladies' in cat_lower or 'female' in cat_lower:
            return "Women's"
        elif 'amateur' in cat_lower:
            return "Men's Amateur"
        elif 'men' in cat_lower or 'male' in cat_lower:
            return "Men's"
        return cat  # Return as-is if already a valid category
    
    # Try to extract from name
    name = str(row.get('name', '')).lower() if pd.notna(row.get('name')) else ""
    
    if re.search(r'\bjunior\b|\bjr\.?\b|\byouth\b|\bboys\b|\bgirls\b', name):
        return 'Junior'
    elif re.search(r'\bwomen\'?s\b|\bladies\b|\bfemale\b', name):
        return "Women's"
    elif re.search(r'\bsenior\b|\bsr\.?\b|\bsuper.?senior\b', name):
        return 'Senior'
    elif re.search(r'\bamateur\b|\bam\b', name):
        return "Men's Amateur"
    elif re.search(r'\bmen\'?s\b|\bmale\b', name):
        return "Men's"
    
    return None  # Return None if no category detected


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
    expected_columns = ['date', 'name', 'course', 'category', 'city', 'state', 'zip']
    
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
    
    # Extract/clean category
    cleaned_df['category'] = cleaned_df.apply(extract_category, axis=1)
    
    # Reorder columns
    final_columns = ['date', 'name', 'course', 'category', 'city', 'state', 'zip']
    other_columns = [col for col in cleaned_df.columns if col not in final_columns]
    cleaned_df = cleaned_df[final_columns + other_columns]
    
    # Capitalize column names for display
    cleaned_df.columns = [col.replace('_', ' ').title() for col in cleaned_df.columns]
    
    return cleaned_df


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
    return combined_text[:25000]


def parse_tournaments_with_ai(text_content, api_key):
    """Use OpenAI to parse tournament data from text."""
    
    client = OpenAI(api_key=api_key)
    
    prompt = """You are a data extraction expert. Extract ALL golf tournament information from the following webpage content.

IMPORTANT: Extract EVERY tournament entry you find. Do not skip any. Each row/entry typically contains:
- A date (like "Jan 13", "Feb 7-8", "Mar 2", etc.)
- A tournament/event name (may include "*FULL*" marker)
- A golf course or venue name
- Location info (city, state like "FL" or "Florida")

For each tournament found, extract:
- date: The tournament date exactly as shown (e.g., "Jan 13", "Feb 7-8")
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
  {"date": "Jan 13", "name": "Southern Hills Plantation Club", "course": "Southern Hills Plantation Club", "category": null, "city": "Brooksville", "state": "FL", "zip": null},
  {"date": "Feb 7-8", "name": "SW Amateur Series - San Carlos", "course": "San Carlos Golf Club", "category": "Amateur", "city": "Fort Myers", "state": "FL", "zip": null}
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
            max_tokens=8000  # Increased to handle more tournaments
        )
        
        result = response.choices[0].message.content.strip()
        
        # Clean up the response - remove markdown code blocks if present
        if result.startswith('```'):
            result = re.sub(r'^```(?:json)?\n?', '', result)
            result = re.sub(r'\n?```$', '', result)
        
        # Parse JSON
        tournaments = json.loads(result)
        return tournaments
        
    except json.JSONDecodeError as e:
        st.error(f"Error parsing AI response: {str(e)}")
        st.text("Raw response:")
        st.code(result)
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
        - Enter a tournament schedule URL
        - AI extracts tournament data automatically
        - Works with most golf tournament websites
        
        **Option 2: CSV Upload**
        - Upload your own CSV file
        - Data gets cleaned and standardized
        
        **Columns extracted:**
        - Date, Name, Course
        - Category, City, State, Zip
        """)
    
    # Main content with tabs
    tab1, tab2 = st.tabs(["🌐 Parse from URL", "📄 Upload CSV"])
    
    # --- TAB 1: URL Parsing ---
    with tab1:
        st.markdown("### Enter a tournament schedule URL")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            url = st.text_input(
                "URL",
                placeholder="https://www.fsga.org/tournaments/schedule...",
                label_visibility="collapsed"
            )
        with col2:
            parse_button = st.button("🔍 Extract Data", type="primary", use_container_width=True)
        
        # Example URLs
        with st.expander("📌 Example URLs"):
            example_urls = [
                "https://www.fsga.org/TournamentCategory/EnterList/d99ad47f-2e7d-4ff4-8a32-c5b1eb315d28?year=2025",
                "https://wpga-onlineregistration.golfgenius.com/pages/1264528",
                "https://usamtour.bluegolf.com/bluegolf/usamtour25/schedule/index.htm",
            ]
            for ex_url in example_urls:
                st.code(ex_url, language=None)
        
        if parse_button:
            if not url:
                st.error("Please enter a URL")
            elif not api_key:
                st.error("Please enter your OpenAI API key in the sidebar")
            else:
                result_df = process_url_with_ai(url, api_key)
                
                if result_df is not None and len(result_df) > 0:
                    st.session_state['url_results'] = result_df
                    st.success(f"✅ Found {len(result_df)} tournaments!")
        
        # Display results
        if 'url_results' in st.session_state and st.session_state['url_results'] is not None:
            df = st.session_state['url_results']
            
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
                
                # Download options
                st.markdown("### 📥 Download Cleaned Data")
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


if __name__ == "__main__":
    main()
