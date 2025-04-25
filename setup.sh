#!/bin/bash
# Simple setup script for Golf Tournament Scraper

echo "Setting up Golf Tournament Scraper..."

# Create virtual environment (optional)
if command -v python3 &>/dev/null; then
    echo "Python 3 is installed."
    
    # Try to create a virtual environment (optional)
    if [ ! -d "venv" ]; then
        echo "Creating virtual environment..."
        python3 -m venv venv || echo "Virtual environment creation failed, continuing without it."
    fi
    
    # Activate virtual environment if it exists
    if [ -d "venv" ]; then
        echo "Activating virtual environment..."
        source venv/bin/activate || echo "Could not activate virtual environment, continuing without it."
    fi
    
    # Install dependencies using pip
    echo "Installing dependencies..."
    pip install -r requirements.txt
    
    echo "Setup completed! Run the app with: streamlit run app.py"
else
    echo "Python 3 is not installed. Please install Python 3 and try again."
    exit 1
fi
