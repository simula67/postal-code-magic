import math
import logging
from typing import List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import os
import duckdb
import itertools
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='zipcode_distance.log',
    filemode='a'
)

# Configuration
INPUT_CSV = 'zipcodes.csv'  # Input CSV file
DB_FILE = 'zipcode_distances.duckdb'  # DuckDB database file
RESULTS_TABLE = 'calculated_distances'  # Table to store results
MAX_WORKERS = 4  # Number of threads for parallel processing


# Function to calculate the Haversine distance
def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0  # Earth's radius in kilometers
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


# Function to calculate distance for a single pair of ZIP codes
def calculate_distance(pair: Tuple[Tuple[str, float, float], Tuple[str, float, float]]) -> Optional[
    Tuple[str, str, float]]:
    try:
        zip1, lat1, lon1 = pair[0]
        zip2, lat2, lon2 = pair[1]
        distance = haversine(lat1, lon1, lat2, lon2)
        logging.info(f"Distance between {zip1} and {zip2}: {distance:.2f} km")
        return zip1, zip2, distance
    except Exception as e:
        logging.error(f"Error calculating distance for {pair}: {e}")
        return None


# Function to save results to DuckDB
def save_results_to_duckdb(results: List[Tuple[str, str, float]]):
    try:
        conn = duckdb.connect(DB_FILE)
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
                zip1 TEXT,
                zip2 TEXT,
                distance_km FLOAT,
                PRIMARY KEY (zip1, zip2)
            )
        """)
        conn.executemany(
            f"INSERT OR IGNORE INTO {RESULTS_TABLE} (zip1, zip2, distance_km) VALUES (?, ?, ?)", results
        )
        conn.close()
    except Exception as e:
        logging.error(f"Error saving results to DuckDB: {e}")


# Function to load completed pairs from DuckDB
def load_completed_pairs() -> set:
    try:
        conn = duckdb.connect(DB_FILE)
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
                zip1 TEXT,
                zip2 TEXT,
                distance_km FLOAT,
                PRIMARY KEY (zip1, zip2)
            )
        """)
        completed_pairs = set(
            conn.execute(f"SELECT zip1, zip2 FROM {RESULTS_TABLE}").fetchall()
        )
        conn.close()
        return completed_pairs
    except Exception as e:
        logging.error(f"Error loading completed pairs from DuckDB: {e}")
        return set()


# Parallelized function for calculating distances for all pairs
def calculate_distances_for_all_pairs(zip_data: List[Tuple[str, float, float]]):
    # Generate all possible pairs
    all_pairs = list(itertools.combinations(zip_data, 2))

    # Load completed pairs
    completed_pairs = load_completed_pairs()
    remaining_pairs = [
        pair for pair in all_pairs
        if (pair[0][0], pair[1][0]) not in completed_pairs and (pair[1][0], pair[0][0]) not in completed_pairs
    ]

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_pair = {
            executor.submit(calculate_distance, pair): pair
            for pair in remaining_pairs
        }

        with tqdm(total=len(remaining_pairs)) as pbar:
            for future in as_completed(future_to_pair):
                pair = future_to_pair[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    logging.error(f"Error processing pair {pair}: {e}")
                finally:
                    # Save results in batches for fault tolerance
                    if len(results) >= 100:
                        save_results_to_duckdb(results)
                        results.clear()
                    pbar.update(1)

    # Save any remaining results
    if results:
        save_results_to_duckdb(results)


# Main function
if __name__ == "__main__":
    # Load ZIP code data
    zip_data_df = pd.read_csv(INPUT_CSV)
    zip_data = zip_data_df[['zipcode', 'latitude', 'longitude']].values.tolist()

    # Calculate distances for all pairs
    calculate_distances_for_all_pairs(zip_data)

    print(f"Calculation completed. Results are stored in '{DB_FILE}' in the '{RESULTS_TABLE}' table.")