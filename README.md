# Golf Tournament Scraper - Installation Guide

## Issue Overview

You're encountering errors when trying to install the Golf Tournament Scraper application, specifically with packages that require C extensions like Pillow and pandas. Based on the error logs, your environment is missing the zlib development library, which is preventing Pillow from building.

## Simplified Installation

I've created a minimal version of the application that requires only three dependencies:
- streamlit
- requests
- beautifulsoup4

These packages don't require C compilation and should install cleanly in most environments.

## Installation Steps

1. First, make sure you have Python 3.7+ installed.

2. Create a new directory for your project:
```bash
mkdir golf-scraper
cd golf-scraper
```

3. Copy the minimal requirements.txt and the minimal-streamlit-app.py files into this directory.

4. Install the dependencies:
```bash
pip install -r requirements.txt
```

5. Run the application:
```bash
streamlit run minimal-streamlit-app.py
```

## If You Still Encounter Issues

If you still have installation problems, you might need to install system-level dependencies. On Ubuntu/Debian systems:

```bash
# Update package lists
sudo apt-get update

# Install Python development files
sudo apt-get install -y python3-dev

# Install pip
sudo apt-get install -y python3-pip

# Install streamlit directly (might work better than through pip)
pip install --user streamlit
pip install --user requests
pip install --user beautifulsoup4
```

## Using a Pre-built Environment

Another option is to use a pre-built environment like Google Colab or Streamlit Cloud, where dependencies are already installed:

1. **Streamlit Cloud**: 
   - Create a GitHub repository with your code
   - Deploy directly from Streamlit Cloud (https://streamlit.io/cloud)

2. **Google Colab with Streamlit**:
   - Create a new Colab notebook
   - Install and run Streamlit within Colab
   ```python
   !pip install streamlit
   !pip install requests beautifulsoup4
   
   # Write your app to a file
   %%writefile app.py
   # Paste your application code here
   
   # Run streamlit
   !streamlit run app.py & npx localtunnel --port 8501
   ```

## Features in the Minimal Version

The minimal version includes:
- Tournament scraping from association websites
- Support for various HTML structures (tables, lists, divs)
- Detail page scraping
- Qualifier detection
- CSV export
- Progress updates while scraping
- Filtering by tournament type and qualifier status

This version avoids dependencies on pandas and Pillow, which were causing the installation issues.
