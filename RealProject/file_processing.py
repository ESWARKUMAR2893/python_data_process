import argparse
import json
import os
import pandas as pd
from datetime import datetime


def load_json(file_name):
    """
    Load and return the JSON data from the given file.

    :param file_name: Name of the JSON file to be loaded.
    :return: Parsed JSON data.
    """
    with open(file_name, 'r') as file:
        return json.load(file)


def read_database_config(file_name, number):
    """
    Read the database configuration from a JSON file for the given number.

    :param file_name: Name of the JSON file containing the configuration.
    :param number: The key in the JSON file for which the config is to be read.
    :return: The type of the database from the configuration.
    """
    print(f"\nReading {file_name} file ..")
    config = load_json(file_name)
    db_type = config[str(number)]["type"]
    print(f"\n Extracted Database type is {db_type} \n")
    return db_type


def read_file_config(file_name, number):
    """
    Read the file configuration from a JSON file for the given number.

    :param file_name: Name of the JSON file containing the configuration.
    :param number: The key in the JSON file for which the config is to be read.
    :return: A tuple containing file info and file extension.
    """
    print(f"\nReading {file_name} file ...")
    config = load_json(file_name)
    file_info = config[str(number)]
    file_name = file_info["file_name"]
    file_extension = os.path.splitext(file_name)[1][1:]  # Extract extension from file_name
    delimiter = file_info.get("delimiter", "")  # Get delimiter from file_info, default to empty string if not present
    print(f"Extracted file name: {file_name}  with Delimiter: '{delimiter}' \n")
    return file_name, delimiter, file_extension


def read_file_output_config(file_name, number):
    """
    Read the output file configuration from a JSON file for the given number.

    :param file_name: Name of the JSON file containing the configuration.
    :param number: The key in the JSON file for which the config is to be read.
    :return: The type of the output from the configuration.
    """
    print(f"\nReading {file_name} file ..")
    config = load_json(file_name)
    file_type = config[str(number)]
    print(f"Extracted File type which we need to convert is {file_type} type \n")
    return file_type


def read_api_config(file_name, number):
    """
    Read the API configuration from a JSON file for the given number.

    :param file_name: Name of the JSON file containing the configuration.
    :param number: The key in the JSON file for which the config is to be read.
    :return: The name of the API from the configuration.
    """
    print(f"Reading {file_name} file")
    config = load_json(file_name)
    api_name = config[str(number)]
    print(f"Extracted API name from file is {api_name} \n")
    return api_name


# Define utility functions for each conversion
def file_to_db(source,target):
    print(f"We are in a function file_to_db to Extract {source[0][0]} and convert to {target[0]} DB\n")
    print("Work in progress....")


def transform_data(data_frame):
    # Adjust the columns dynamically based on the input data
    required_columns = {'name', 'age', 'date_of_joining', 'id', 'author', 'title', 'genre', 'price', 'publish_date'}
    columns_to_keep = [col for col in data_frame.columns if col in required_columns]
    return data_frame[columns_to_keep]


def extract_csv(file_path, delimiter):
    return pd.read_csv(file_path, delimiter=delimiter)


def file_to_output(source,target):
    print(f"We are in a function file_to_output to Extract {source[0][0]} and convert to {target[0]} format")
    # print("^^^^^^^^^^^^^^^^^^^^^^^^^^^")
    #print(source)
    # print("Target",target[0])
    output_format = target[0]
    filename = source[0][0]
    delimiter = source[0][1]
    print("filename is :",filename)
    print("delimiter is :",delimiter)
    data = extract_csv(filename, delimiter)
    data_frame = transform_data(data)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"output_{timestamp}.{output_format}"
    if output_format == 'csv':
        data_frame.to_csv(filename, index=False, sep=delimiter)
    elif output_format == 'txt':
        data_frame.to_csv(filename, index=False, sep=delimiter)
    elif output_format == 'json':
        data_frame.to_json(filename, orient='records', lines=True)
    else:
        raise ValueError(f"Unsupported output format: {output_format}")

    print(f"{filename} Created Successfully..")


def file_to_api():
    print("Calling File to API Utility....")


def db_to_file():
    print("Calling DB to File Utility....")


def db_to_output():
    print("Calling DB to File Conversion Logic....")


def db_to_api():
    print("Calling DB to API Utility....")


def output_to_file():
    print("Calling Output to File Utility....")


def output_to_db():
    print("Calling Output to DB Utility....")


def output_to_api():
    print("Calling Output to API Utility....")


def api_to_file():
    print("Calling API to File Utility....")


def api_to_db():
    print("Calling API to DB Utility....")


def api_to_output():
    print("Calling API to Output Utility....")


def process_file(file, num):
    """
    Process the configuration file based on its type.

    :param file: JSON configuration file name.
    :param num: Key number to access in the JSON file.
    :return: Tuple containing configuration data and its type.
    """
    if 'DbConfig' in file:
        return read_database_config(file, num), "DB"
    elif 'FileConfig' in file:
        return read_file_config(file, num), "FILE"
    elif 'OutputConfig' in file:
        return read_file_output_config(file, num), "OUTPUT"
    elif 'APIConfig' in file:
        return read_api_config(file, num), "API"
    else:
        print(f"Unknown configuration type for {file}")


def main():
    parser = argparse.ArgumentParser(description='Read database type and file info from JSON config files.')

    parser.add_argument('file1', type=str, help='The first JSON configuration file')
    parser.add_argument('num1', type=int, help='The key number to access in the first JSON file')
    parser.add_argument('file2', type=str, help='The second JSON configuration file')
    parser.add_argument('num2', type=int, help='The key number to access in the second JSON file')

    args = parser.parse_args()

    source_main = process_file(args.file1, args.num1)
    target_main = process_file(args.file2, args.num2)
    print("Source:", source_main[-1], "Target:", target_main[-1])
    print(f"\nBy the inputs you passed we need to convert source {source_main[-1]} to target {target_main[-1]} type \n")

    source, target = source_main[-1], target_main[-1]

    # Check the combination and call the appropriate function
    if source == target:
        print(f"This Combination is not allowed: Source and Target both are {source}")
    else:
        if source == "FILE":
            if target == "DB":
                file_to_db(source_main,target_main)
            elif target == "OUTPUT":
                file_to_output(source_main,target_main)
            elif target == "API":
                file_to_api()
        elif source == "DB":
            if target == "FILE":
                print("This Combination is not allowed: From DB to FILE ")
                #db_to_file()
            elif target == "OUTPUT":
                db_to_output()
            elif target == "API":
                db_to_api()
        elif source == "OUTPUT":
            if target == "FILE":
                output_to_file()
            elif target == "DB":
                output_to_db()
            elif target == "API":
                output_to_api()
        elif source == "API":
            if target == "FILE":
                api_to_file()
            elif target == "DB":
                api_to_db()
            elif target == "OUTPUT":
                api_to_output()


if __name__ == '__main__':
    main()
