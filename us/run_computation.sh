#!/bin/bash

# Configuration
VENV_DIR=".venv"                     # Virtual environment directory
REQUIREMENTS_FILE="requirements.txt"
INPUT_CSV="zipcodes.csv"           # Input CSV file
DB_FILE="zipcode_distances.duckdb" # DuckDB database file
RESULTS_TABLE="calculated_distances" # Table to store results
COMPUTATION_SCRIPT="calculate_distances.py" # Computation script

# Dependencies
DEPENDENCIES=(
  "duckdb"
  "pandas"
  "tqdm"
)

# Function to create a virtual environment
create_virtualenv() {
  if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment in '$VENV_DIR'..."
    python3 -m venv "$VENV_DIR"
    echo "Virtual environment created."
  else
    echo "Virtual environment '$VENV_DIR' already exists."
  fi
}

# Function to write requirements.txt
write_requirements() {
  if [ ! -f "$REQUIREMENTS_FILE" ]; then
    echo "Writing requirements to '$REQUIREMENTS_FILE'..."
    >"$REQUIREMENTS_FILE"
    for dep in "${DEPENDENCIES[@]}"; do
      echo "$dep" >>"$REQUIREMENTS_FILE"
    done
    echo "Requirements written to '$REQUIREMENTS_FILE'."
  fi
}

# Function to install dependencies in the virtual environment
install_dependencies() {
  echo "Installing dependencies in the virtual environment..."
  source "$VENV_DIR/bin/activate"
  pip install --upgrade pip
  pip install -r "$REQUIREMENTS_FILE"
  deactivate
  echo "Dependencies installed."
}

# Function to validate the input CSV file
validate_input_csv() {
  if [ ! -f "$INPUT_CSV" ]; then
    echo "Error: Input file '$INPUT_CSV' not found. Please provide a valid file."
    exit 1
  fi

  echo "Validating input CSV..."
  source "$VENV_DIR/bin/activate"
  python3 -c "
import pandas as pd
try:
    df = pd.read_csv('$INPUT_CSV')
    required_columns = {'zipcode', 'latitude', 'longitude'}
    if not required_columns.issubset(df.columns):
        raise ValueError(f'Missing required columns: {required_columns - set(df.columns)}')
    print(f'Input CSV is valid with {len(df)} rows.')
except Exception as e:
    raise SystemExit(f'Error validating input CSV: {e}')
"
  deactivate
}

# Function to initialize the DuckDB database
initialize_duckdb() {
  echo "Initializing DuckDB database..."
  source "$VENV_DIR/bin/activate"
  python3 -c "
import duckdb
conn = duckdb.connect('$DB_FILE')
conn.execute(f'''
    CREATE TABLE IF NOT EXISTS $RESULTS_TABLE (
        zip1 TEXT,
        zip2 TEXT,
        distance_km FLOAT,
        PRIMARY KEY (zip1, zip2)
    )
''')
print(f'Initialized DuckDB database \\'$DB_FILE\\' with table \\'$RESULTS_TABLE\\'.')
conn.close()
"
  deactivate
}

# Function to run the computation script
run_computation() {
  echo "Running computation..."
  source "$VENV_DIR/bin/activate"
  python3 "$COMPUTATION_SCRIPT"
  deactivate
  echo "Computation completed."
}

# Main function
main() {
  echo "Starting setup and computation..."

  # Step 1: Create virtual environment if not exists
  create_virtualenv

  # Step 2: Write requirements.txt if not exists
  write_requirements

  # Step 3: Install dependencies in virtual environment
  install_dependencies

  # Step 4: Validate the input CSV file
  validate_input_csv

  # Step 5: Initialize the DuckDB database
  initialize_duckdb

  # Step 6: Run the computation script
  run_computation

  echo "Setup and computation completed successfully."
}

# Run the main function
main