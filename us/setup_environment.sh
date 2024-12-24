#!/bin/bash

# Set script to fail on errors
set -e

# Define the virtual environment directory
VENV_DIR=".venv"

# Step 1: Create a virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
    echo "Virtual environment created."
else
    echo "Virtual environment already exists. Skipping creation."
fi

# Step 2: Activate the virtual environment
echo "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

# Step 3: Install required dependencies
echo "Installing dependencies..."
pip install --upgrade pip
pip install pandas requests
echo "Dependencies installed."

# Step 4: Run the prepare_zipcodes_data.py script
echo "Running prepare_zipcodes_data.py..."
python prepare_zipcodes_data.py

# Deactivate the virtual environment
echo "Deactivating virtual environment..."
deactivate

echo "Setup and execution completed successfully."