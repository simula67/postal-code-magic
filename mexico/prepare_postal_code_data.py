import os
import logging
import pandas as pd
import requests
from tqdm import tqdm

# File paths
MEXICO_ZIPCODES_URL = "https://download.geonames.org/export/zip/MX.zip"
RAW_ZIPCODES_FILE = "MX.zip"
EXTRACTED_FILE = "MX.txt"
PROCESSED_ZIPCODES_FILE = "mexico_postalcodes.csv"

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


def download_and_extract_zipcodes():
    """Download and extract Mexican postal codes data."""
    if os.path.exists(PROCESSED_ZIPCODES_FILE):
        logging.info(f"Processed file '{PROCESSED_ZIPCODES_FILE}' already exists. Skipping download.")
        return

    # Download ZIP codes file
    if not os.path.exists(RAW_ZIPCODES_FILE):
        logging.info(f"Downloading postal codes data from {MEXICO_ZIPCODES_URL}...")
        response = requests.get(MEXICO_ZIPCODES_URL, stream=True, verify=False)
        response.raise_for_status()
        with open(RAW_ZIPCODES_FILE, "wb") as f:
            total_size = int(response.headers.get('content-length', 0))
            with tqdm(total=total_size, unit="B", unit_scale=True, desc="Downloading MX.zip") as pbar:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
                    pbar.update(len(chunk))
        logging.info("Download complete.")

    # Extract postal codes file
    if not os.path.exists(EXTRACTED_FILE):
        logging.info(f"Extracting {RAW_ZIPCODES_FILE}...")
        import zipfile
        with zipfile.ZipFile(RAW_ZIPCODES_FILE, 'r') as zip_ref:
            zip_ref.extractall(".")
        logging.info("Extraction complete.")


def process_zipcodes():
    """Process extracted Mexican postal codes data."""
    if not os.path.exists(EXTRACTED_FILE):
        logging.error(f"Extracted file '{EXTRACTED_FILE}' not found.")
        raise FileNotFoundError(f"{EXTRACTED_FILE} not found. Please ensure the file is downloaded and extracted.")

    logging.info(f"Processing postal codes data from '{EXTRACTED_FILE}'...")
    column_names = [
        "country_code", "postal_code", "place_name", "admin_name1", "admin_code1",
        "admin_name2", "admin_code2", "admin_name3", "admin_code3", "latitude",
        "longitude", "accuracy"
    ]
    df = pd.read_csv(EXTRACTED_FILE, sep="\t", names=column_names, dtype=str)

    # Remove spaces in postal codes
    df["postal_code"] = df["postal_code"].str.replace(" ", "", regex=False)

    # Save processed data
    logging.info(f"Saving processed postal codes to '{PROCESSED_ZIPCODES_FILE}'...")
    df.to_csv(PROCESSED_ZIPCODES_FILE, index=False)
    logging.info(f"Processed postal codes saved to '{PROCESSED_ZIPCODES_FILE}'.")


def main():
    """Main function to prepare Mexican postal codes data."""
    try:
        download_and_extract_zipcodes()
        process_zipcodes()
        logging.info("Mexican postal codes preparation completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    main()