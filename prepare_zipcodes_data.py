import os
import logging
import requests
import pandas as pd
import zipfile

# Configuration
GEONAMES_URL = "https://download.geonames.org/export/zip/US.zip"
OUTPUT_ZIP = "US.zip"
EXTRACT_DIR = "us_zip_codes"
OUTPUT_CSV = "zipcodes.csv"

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


def download_geonames_data():
    """Download the Geonames ZIP code dataset."""
    if os.path.exists(OUTPUT_ZIP):
        logging.info(f"ZIP file '{OUTPUT_ZIP}' already exists. Skipping download.")
        return

    logging.info(f"Downloading ZIP code data from {GEONAMES_URL}...")
    try:
        response = requests.get(GEONAMES_URL, stream=True)
        response.raise_for_status()
        with open(OUTPUT_ZIP, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)
        logging.info(f"Downloaded data to '{OUTPUT_ZIP}'.")
    except requests.RequestException as e:
        logging.error(f"Failed to download data: {e}")
        raise


def extract_zip_file():
    """Extract the ZIP file to the specified directory."""
    if os.path.exists(EXTRACT_DIR):
        logging.info(f"Extract directory '{EXTRACT_DIR}' already exists. Skipping extraction.")
        return

    logging.info(f"Extracting '{OUTPUT_ZIP}' to '{EXTRACT_DIR}'...")
    try:
        with zipfile.ZipFile(OUTPUT_ZIP, "r") as zip_ref:
            zip_ref.extractall(EXTRACT_DIR)
        logging.info(f"Extraction completed.")
    except zipfile.BadZipFile as e:
        logging.error(f"Failed to extract ZIP file: {e}")
        raise


def process_geonames_data():
    """Process the Geonames data and create the `zipcodes.csv` file."""
    txt_file = os.path.join(EXTRACT_DIR, "US.txt")
    if not os.path.exists(txt_file):
        logging.error(f"Expected file '{txt_file}' not found in '{EXTRACT_DIR}'.")
        raise FileNotFoundError(f"File '{txt_file}' is missing.")

    logging.info(f"Processing data from '{txt_file}'...")
    try:
        # Load the Geonames dataset
        df = pd.read_csv(txt_file, sep="\t", header=None, names=[
            "country_code", "postal_code", "place_name", "admin_name1",
            "admin_code1", "admin_name2", "admin_code2",
            "nan1", "nan2", "latitude", "longitude", "accuracy"
        ], index_col=False)

        # Verify the columns in the DataFrame
        logging.debug(f"Columns in loaded data: {list(df.columns)}")

        # Select and rename required columns
        selected_df = df[["postal_code", "latitude", "longitude"]].copy()
        selected_df.rename(columns={"postal_code": "zipcode"}, inplace=True)

        # Verify the processed DataFrame
        logging.debug(f"Processed DataFrame head:\n{selected_df.head()}")

        # Save to CSV
        logging.info(f"Saving processed data to '{OUTPUT_CSV}'...")
        selected_df.to_csv(OUTPUT_CSV, index=False)
        logging.info(f"Data successfully saved to '{OUTPUT_CSV}'.")
    except Exception as e:
        logging.error(f"Error processing Geonames data: {e}")
        raise


def main():
    """Main function to orchestrate the data preparation process."""
    logging.info("Starting ZIP code data preparation...")
    try:
        download_geonames_data()
        extract_zip_file()
        process_geonames_data()
        logging.info("ZIP code data preparation completed successfully.")
    except Exception as e:
        logging.error(f"Data preparation failed: {e}")
        raise


if __name__ == "__main__":
    main()