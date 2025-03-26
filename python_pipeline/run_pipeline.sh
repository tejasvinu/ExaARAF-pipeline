#!/bin/bash
# Change to the script's directory to ensure relative paths work correctly
cd "$(dirname "$0")" || exit 1

# Create a Python virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python -m venv venv
fi

# Activate the virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Run the pipeline
echo "Starting pipeline..."
python main.py
