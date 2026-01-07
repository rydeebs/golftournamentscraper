import streamlit as st
import pandas as pd
import re
from datetime import datetime
import io
import base64

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


# --- Streamlit UI ---
def main():
    st.set_page_config(
        page_title="Golf Tournament Data Cleaner",
        page_icon="⛳",
        layout="wide"
    )
    
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
        </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<p class="main-header">⛳ Golf Tournament Data Cleaner</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Upload a CSV file with tournament data to clean and standardize it.</p>', unsafe_allow_html=True)
    
    # Sidebar with instructions
    with st.sidebar:
        st.header("📋 Instructions")
        st.markdown("""
        **Expected Columns:**
        - **Date** - Tournament date
        - **Name** - Tournament name
        - **Course** - Golf course name
        - **Category** - Senior, Men's, Women's, Junior, etc.
        - **City** - City name
        - **State** - State (name or abbreviation)
        - **Zip** - ZIP code
        
        **What gets cleaned:**
        - Dates → YYYY-MM-DD format
        - States → 2-letter abbreviations
        - ZIP codes → 5-digit format
        - Names → Remove *FULL*, extra spaces
        - Categories → Standardized labels
        """)
        
        st.divider()
        st.markdown("**💡 Tips:**")
        st.markdown("""
        - Column names are matched flexibly
        - Missing columns will be added
        - Categories are auto-detected from names
        """)
    
    # File uploader
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
            st.subheader("📄 Original Data")
            st.dataframe(df, use_container_width=True, height=300)
            
            # Show original stats
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Rows", len(df))
            with col2:
                st.metric("Total Columns", len(df.columns))
            with col3:
                st.metric("Columns Found", ", ".join(df.columns[:4]) + ("..." if len(df.columns) > 4 else ""))
            
            st.divider()
            
            # Clean the data
            with st.spinner("Cleaning data..."):
                cleaned_df = clean_tournament_data(df)
            
            # Display cleaned data
            st.subheader("✨ Cleaned Data")
            st.dataframe(cleaned_df, use_container_width=True, height=400)
            
            # Show cleaning stats
            st.markdown("### 📊 Cleaning Summary")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                valid_dates = cleaned_df['Date'].notna().sum()
                st.markdown(f"""
                <div class="stats-box">
                    <strong>Valid Dates</strong><br>
                    {valid_dates} / {len(cleaned_df)}
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                valid_names = cleaned_df['Name'].notna().sum()
                st.markdown(f"""
                <div class="stats-box">
                    <strong>Valid Names</strong><br>
                    {valid_names} / {len(cleaned_df)}
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                valid_states = cleaned_df['State'].notna().sum()
                st.markdown(f"""
                <div class="stats-box">
                    <strong>Valid States</strong><br>
                    {valid_states} / {len(cleaned_df)}
                </div>
                """, unsafe_allow_html=True)
            
            with col4:
                valid_categories = cleaned_df['Category'].notna().sum()
                st.markdown(f"""
                <div class="stats-box">
                    <strong>Categories Found</strong><br>
                    {valid_categories} / {len(cleaned_df)}
                </div>
                """, unsafe_allow_html=True)
            
            # Category breakdown
            if valid_categories > 0:
                st.markdown("### 🏷️ Category Breakdown")
                category_counts = cleaned_df['Category'].value_counts()
                col1, col2 = st.columns([1, 2])
                with col1:
                    st.dataframe(category_counts.reset_index().rename(columns={'index': 'Category', 'Category': 'Count'}))
                with col2:
                    st.bar_chart(category_counts)
            
            st.divider()
            
            # Download options
            st.subheader("📥 Download Cleaned Data")
            
            col1, col2, col3 = st.columns([1, 1, 2])
            
            with col1:
                st.markdown(get_csv_download_link(cleaned_df), unsafe_allow_html=True)
            
            with col2:
                st.markdown(get_excel_download_link(cleaned_df), unsafe_allow_html=True)
            
            # Preview of changes
            with st.expander("🔍 View Side-by-Side Comparison"):
                comparison_col1, comparison_col2 = st.columns(2)
                with comparison_col1:
                    st.markdown("**Original**")
                    st.dataframe(df.head(10), use_container_width=True)
                with comparison_col2:
                    st.markdown("**Cleaned**")
                    st.dataframe(cleaned_df.head(10), use_container_width=True)
                    
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
            st.exception(e)
    
    else:
        # Show sample data format
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
            
            st.markdown("**After cleaning, this becomes:**")
            cleaned_sample = clean_tournament_data(sample_data)
            st.dataframe(cleaned_sample, use_container_width=True)


if __name__ == "__main__":
    main()
