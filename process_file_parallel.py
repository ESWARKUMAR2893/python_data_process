import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from source_config import FILES

# Configure logging
logging.basicConfig(filename='file_processor.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')


def load_config(config_path):
    try:
        with open(config_path, 'r') as file:
            config = json.load(file)
            logging.info("Configuration file loaded successfully.")
            return config
    except Exception as e:
        logging.error(f"Error loading configuration file: {e}")
        raise

def process_csv(file_path, delimiter, chunk_size=1000):
    try:
        chunk_list = []
        for chunk in pd.read_csv(file_path, delimiter=delimiter, chunksize=chunk_size):
            chunk_list.append(chunk)
            logging.info(f"Processed chunk of CSV file {file_path} with shape {chunk.shape}")
        df = pd.concat(chunk_list, ignore_index=True)
        logging.info(f"CSV file {file_path} read successfully with delimiter '{delimiter}'. Final shape: {df.shape}")
        return df
    except Exception as e:
        logging.error(f"Error reading CSV file {file_path}: {e}")
        raise


def process_txt(file_path, delimiter, chunk_size=1000):
    try:
        chunk_list = []
        for chunk in pd.read_csv(file_path, delimiter=delimiter, chunksize=chunk_size):
            chunk_list.append(chunk)
            logging.info(f"Processed chunk of TXT file {file_path} with shape {chunk.shape}")
        df = pd.concat(chunk_list, ignore_index=True)
        logging.info(f"TXT file {file_path} read successfully with delimiter '{delimiter}'. Final shape: {df.shape}")
        return df
    except Exception as e:
        logging.error(f"Error reading TXT file {file_path}: {e}")
        raise


def main():
    files_config = FILES

    if not files_config:
        logging.error("No files configuration found in the config file.")
        return

    tasks = []
    with ThreadPoolExecutor() as executor:
        # Process CSV file
        csv_config = files_config.get('csv')
        if csv_config:
            if 'path' in csv_config and 'delimiter' in csv_config:
                tasks.append(executor.submit(process_csv, csv_config['path'], csv_config['delimiter']))
            else:
                logging.error("CSV configuration is missing 'path' or 'delimiter'.")

        # Process TXT file
        txt_config = files_config.get('txt')
        if txt_config:
            path = txt_config.get('path')
            delimiter = txt_config.get('delimiter', '\t')  # Default to tab delimiter if not specified
            if path:
                tasks.append(executor.submit(process_txt, path, delimiter))
            else:
                logging.error("TXT configuration is missing 'path'.")

        for future in as_completed(tasks):
            try:
                result = future.result()
                # Log the result if needed (for large data, consider logging just a summary)
                logging.info(f"Processed file with result: {type(result).__name__}, shape: {result.shape}")
            except Exception as e:
                logging.error(f"Error in task: {e}")


if __name__ == "__main__":
    main()
