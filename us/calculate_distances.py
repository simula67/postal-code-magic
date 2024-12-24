import itertools
import logging
import os
import duckdb
import pandas as pd
from geopy.distance import geodesic
from tqdm import tqdm

# File paths
ZIPCODES_FILE = "us_zipcodes.csv"
RESULTS_DB = "distances.duckdb"
RESULTS_TABLE = "zip_distances"
PAIRS_TABLE = "zip_pairs"

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


def load_zipcodes():
    """Load ZIP codes and their coordinates."""
    if not os.path.exists(ZIPCODES_FILE):
        logging.error(f"ZIP codes file '{ZIPCODES_FILE}' not found.")
        raise FileNotFoundError(f"{ZIPCODES_FILE} not found.")

    logging.info(f"Loading ZIP codes from '{ZIPCODES_FILE}'...")
    zipcodes = pd.read_csv(ZIPCODES_FILE)
    if not {"zipcode", "latitude", "longitude"}.issubset(zipcodes.columns):
        logging.error(f"Missing required columns in '{ZIPCODES_FILE}'.")
        raise ValueError("ZIP codes file must have 'zipcode', 'latitude', and 'longitude' columns.")

    logging.info(f"Loaded {len(zipcodes)} ZIP codes.")
    return zipcodes


def initialize_pairs_table(zipcodes, conn):
    """Initialize or resume the ZIP code pairs table."""
    logging.info("Initializing or resuming ZIP code pairs...")

    # Ensure pairs table exists
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {PAIRS_TABLE} (
            zip1 TEXT,
            zip2 TEXT,
            processed BOOLEAN DEFAULT FALSE
        )
    """)

    # Check if table already has pairs
    existing_count = conn.execute(f"SELECT COUNT(*) FROM {PAIRS_TABLE}").fetchone()[0]
    if existing_count == 0:
        logging.info("Generating and saving all unique pairs...")

        # Generate all pairs and insert them into the table
        pairs = itertools.permutations(zipcodes["zipcode"], 2)
        total_pairs = len(zipcodes) * (len(zipcodes) - 1)
        batch = []

        for pair in tqdm(pairs, desc="Generating ZIP code pairs", total=total_pairs, unit="pair"):
            batch.append(pair)
            if len(batch) >= 10000:  # Batch insert every 10,000 pairs
                conn.executemany(f"INSERT INTO {PAIRS_TABLE} (zip1, zip2) VALUES (?, ?)", batch)
                batch = []
        if batch:  # Insert remaining pairs
            conn.executemany(f"INSERT INTO {PAIRS_TABLE} (zip1, zip2) VALUES (?, ?)", batch)

        logging.info("All unique pairs saved to database.")
    else:
        logging.info(f"Resuming from {existing_count} existing pairs.")


def calculate_distances(zipcodes, conn):
    """Calculate distances and save them to the database."""
    logging.info("Calculating distances between ZIP code pairs...")

    # Ensure results table exists
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
            zip1 TEXT,
            zip2 TEXT,
            distance_km FLOAT
        )
    """)

    # Retrieve unprocessed pairs
    unprocessed_pairs = conn.execute(f"SELECT zip1, zip2 FROM {PAIRS_TABLE} WHERE processed = FALSE").fetchall()
    total_unprocessed_pairs = len(unprocessed_pairs)

    if total_unprocessed_pairs == 0:
        logging.info("All pairs have already been processed.")
        return

    logging.info(f"Found {total_unprocessed_pairs} unprocessed pairs.")

    for zip1, zip2 in tqdm(unprocessed_pairs, desc="Calculating distances", total=total_unprocessed_pairs, unit="pair"):
        # Get coordinates for both ZIP codes
        coord1 = zipcodes[zipcodes["zipcode"] == zip1][["latitude", "longitude"]].values[0]
        coord2 = zipcodes[zipcodes["zipcode"] == zip2][["latitude", "longitude"]].values[0]

        # Calculate geodesic distance
        distance_km = geodesic(coord1, coord2).miles

        # Save to the database
        conn.execute(f"""
            INSERT INTO {RESULTS_TABLE} (zip1, zip2, distance_km)
            VALUES (?, ?, ?)
        """, [zip1, zip2, distance_km])

        # Mark pair as processed
        conn.execute(f"UPDATE {PAIRS_TABLE} SET processed = TRUE WHERE zip1 = ? AND zip2 = ?", [zip1, zip2])


def main():
    """Main function to orchestrate the distance calculation."""
    logging.info("Starting ZIP code distance calculations...")

    try:
        zipcodes = load_zipcodes()
        conn = duckdb.connect(RESULTS_DB)

        # Initialize or resume ZIP code pairs table
        initialize_pairs_table(zipcodes, conn)

        # Perform calculations and save to database
        calculate_distances(zipcodes, conn)

        logging.info("Distance calculations completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    main()