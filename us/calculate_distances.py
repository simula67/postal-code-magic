import itertools
import logging
import os
import sqlite3
import pandas as pd
from geopy.distance import geodesic
from tqdm import tqdm
import psutil  # For disk space monitoring

# File paths
ZIPCODES_FILE = "us_zipcodes.csv"
RESULTS_DB = "distances.db"
RESULTS_TABLE = "zip_distances"
DISK_SPACE_THRESHOLD_MB = 10240  # Minimum free disk space in MB

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


def check_disk_space():
    """Check available disk space and raise an error if it's below the threshold."""
    free_space = psutil.disk_usage('/').free / (1024 * 1024)  # Convert to MB
    if free_space < DISK_SPACE_THRESHOLD_MB:
        logging.error(f"Low disk space: {free_space:.2f} MB available. Threshold is {DISK_SPACE_THRESHOLD_MB} MB.")
        raise Exception("Insufficient disk space. Please free up space and try again.")


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
    check_disk_space()

    # Ensure pairs table exists and add distance_miles column
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
            zip1 TEXT,
            zip2 TEXT,
            distance_miles FLOAT DEFAULT NULL
        )
    """)


    total_pairs = len(zipcodes) * (len(zipcodes) - 1) // 2
    # Check if table already has pairs
    existing_count = conn.execute(f"SELECT COUNT(*) FROM {RESULTS_TABLE}").fetchone()[0]
    if existing_count == 0:
        logging.info("Generating and saving all unique pairs...")
        pairs = itertools.combinations(zipcodes["zipcode"], 2)
        batch = []

        for pair in tqdm(pairs, desc="Generating ZIP code pairs", total=total_pairs, unit=" pair"):
            batch.append(pair)
            if len(batch) >= 10000:  # Batch insert every 10,000 pairs
                check_disk_space()
                conn.executemany(f"INSERT INTO {RESULTS_TABLE} (zip1, zip2) VALUES (?, ?)", batch)
                batch = []
        if batch:
            check_disk_space()
            conn.executemany(f"INSERT INTO {RESULTS_TABLE} (zip1, zip2) VALUES (?, ?)", batch)

        logging.info(f"Creating indexes on {RESULTS_TABLE} table for zips.")
        # Create index for zip1 and zip2
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_zip1_zip2 ON {RESULTS_TABLE} (zip1, zip2)")
        conn.commit()
        logging.info("All unique pairs saved to database.")
    elif existing_count == total_pairs:
        logging.info(f"Tables already contain all {total_pairs} unique pairs. Skipping generation.")
        return
    else:
        logging.info(f"Cannot resume from {existing_count} existing pairs.")
        raise Exception(
            f'Existing pairs table {existing_count} incomplete (should be {total_pairs}). Please delete the table and try again.')
    logging.info(f"Initialized {total_pairs} ZIP code pairs.")


def calculate_distances(zipcodes, conn, batch_size=1000):
    """Calculate distances and save them to the database."""
    logging.info("Calculating distances between ZIP code pairs...")
    check_disk_space()

    # Ensure results table exists
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
            zip1 TEXT,
            zip2 TEXT,
            distance_miles FLOAT
        )
    """)

    # Get the total number of pairs to process
    total_pairs = conn.execute(f"SELECT COUNT(*) FROM {RESULTS_TABLE} WHERE distance_miles IS NULL").fetchone()[0]

    # Process pairs in batches
    total_floored_pairs = total_pairs // batch_size
    if total_pairs % batch_size == 0:
        num_batches = total_floored_pairs
    else:
        num_batches = total_floored_pairs + 1
    with tqdm(total=num_batches, desc="Processing ZIP code pairs", unit=" batches") as pbar:
        while total_pairs > 0:
            check_disk_space()

            # Retrieve a batch of pairs
            unprocessed_pairs = conn.execute(f"""
                SELECT zip1, zip2 FROM {RESULTS_TABLE} WHERE distance_miles IS NULL LIMIT {batch_size}
            """).fetchall()

            if not unprocessed_pairs:
                logging.info("All pairs have been processed.")
                break

            # Prepare a list of tuples for the update
            processed_pairs = []
            for zip1, zip2 in unprocessed_pairs:
                # Get coordinates for both ZIP codes
                coord1 = zipcodes.loc[zipcodes["zipcode"] == int(zip1), ["latitude", "longitude"]].iloc[0].values
                coord2 = zipcodes.loc[zipcodes["zipcode"] == int(zip2), ["latitude", "longitude"]].iloc[0].values

                if coord1 is not None and coord2 is not None:
                    # Calculate geodesic distance
                    distance_miles = geodesic(coord1, coord2).miles
                    # Append the update statement
                    processed_pairs.append((distance_miles, zip1, zip2))

            # Perform the update for all pairs in one go
            if processed_pairs:
                conn.executemany(f"""
                    UPDATE {RESULTS_TABLE}
                    SET distance_miles = ?
                    WHERE zip1 = ? AND zip2 = ?
                """, processed_pairs)

            conn.commit()  # Commit after processing a batch
            processed_pairs_count = len(processed_pairs)
            pbar.update(processed_pairs_count)

            # Update total pairs count
            total_pairs -= processed_pairs_count


def main():
    """Main function to orchestrate the distance calculation."""
    logging.info("Starting postal code distance calculations...")

    try:
        check_disk_space()
        zipcodes = load_zipcodes()

        # Connect to SQLite database
        conn = sqlite3.connect(RESULTS_DB)

        # Initialize or resume ZIP code pairs table
        initialize_pairs_table(zipcodes, conn)

        # Perform calculations and save to database in batches
        calculate_distances(zipcodes, conn)

        logging.info("Distance calculations completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise
    finally:
        # Close the database connection
        if 'conn' in locals() and conn:
            conn.close()


if __name__ == "__main__":
    main()