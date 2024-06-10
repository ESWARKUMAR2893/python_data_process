# process_file.py

import source_config
import creds_config
import logging
from datetime import datetime
import argparse
import pandas as pd
from multiprocessing import Pool, cpu_count
import psycopg2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Database connection function
def get_db_connection():
    return psycopg2.connect(
        dbname=creds_config.DATABASE['dbname'],
        user=creds_config.DATABASE['user'],
        password=creds_config.DATABASE['password'],
        host=creds_config.DATABASE['host'],
        port=creds_config.DATABASE['port']
    )


def create_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS processed_data (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        date DATE,
        amount FLOAT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()


def process_chunk(chunk):
    processed_rows = []
    for index, row in chunk.iterrows():
        # print("Index,row",index,row)
        if 'date' in row:
            row['date'] = datetime.strptime(row['date'], '%Y-%m-%d').strftime('%Y-%m-%d')  # Change format for database compatibility
        if 'amount' in row:
            row['amount'] = row['amount'] * 1.1  # Add 10% to the amount
        processed_rows.append(row.to_dict())
    return processed_rows


def insert_data(rows):
    conn = get_db_connection()
    cursor = conn.cursor()
    insert_query = """
    INSERT INTO processed_data (name, date, amount)
    VALUES (%s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    date = EXCLUDED.date,
    amount = EXCLUDED.amount;
    """
    for row in rows:
        cursor.execute(insert_query, (row.get('name'), row.get('date'), row.get('amount')))
    conn.commit()
    cursor.close()
    conn.close()


def process_csv_in_chunks(path, delimiter, chunk_size=1000):
    try:
        columns = ['id', 'name', 'date', 'amount']
        chunks = pd.read_csv(path, delimiter=delimiter, chunksize=chunk_size, names=columns, header=0)
        with Pool(cpu_count()) as pool:
            for result in pool.imap(process_chunk, chunks):
                insert_data(result)
                for row in result:
                    logging.info(f"Processed and inserted row: {row}")
    except FileNotFoundError:
        logging.error(f"File not found: {path}")
    except Exception as e:
        logging.error(f"An error occurred while processing CSV: {e}")

# Process TXT files (no changes needed)
def process_txt(path):
    try:
        with open(path, mode='r') as file:
            for line in file:
                logging.info(line.strip())
    except FileNotFoundError:
        logging.error(f"File not found: {path}")
    except Exception as e:
        logging.error(f"An error occurred while processing TXT: {e}")


def main(file_type):
    if file_type in source_config.FILES:
        config = source_config.FILES[file_type]
        path = config["path"]
        if file_type == "csv":
            delimiter = config["delimiter"]
            create_table()  # Ensure the table exists before processing
            process_csv_in_chunks(path, delimiter)
        elif file_type == "txt":
            process_txt(path)
    else:
        logging.error(f"Unsupported file type: {file_type}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process CSV or TXT files.')
    parser.add_argument('--type', required=True, choices=['csv', 'txt'], help='Type of the source file to process')
    args = parser.parse_args()

    main(args.type)
