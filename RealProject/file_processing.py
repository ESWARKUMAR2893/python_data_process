import argparse
import json
import os


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
    config = load_json(file_name)
    db_type = config[str(number)]["type"]
    print("ReadDB config...", db_type)
    return db_type


def read_file_config(file_name, number):
    """
    Read the file configuration from a JSON file for the given number.

    :param file_name: Name of the JSON file containing the configuration.
    :param number: The key in the JSON file for which the config is to be read.
    :return: A tuple containing file info and file extension.
    """
    config = load_json(file_name)
    file_info = config[str(number)]
    file_extension = os.path.splitext(file_info)[1][1:]
    print("Read File Config", file_info)
    return file_info, file_extension


def read_file_output_config(file_name, number):
    """
    Read the output file configuration from a JSON file for the given number.

    :param file_name: Name of the JSON file containing the configuration.
    :param number: The key in the JSON file for which the config is to be read.
    :return: The type of the output from the configuration.
    """
    config = load_json(file_name)
    file_type = config[str(number)]
    print("Type:", file_type)
    return file_type


def read_api_config(file_name, number):
    """
    Read the API configuration from a JSON file for the given number.

    :param file_name: Name of the JSON file containing the configuration.
    :param number: The key in the JSON file for which the config is to be read.
    :return: The name of the API from the configuration.
    """
    config = load_json(file_name)
    api_name = config[str(number)]
    print("Reading API...", api_name)
    return api_name


# Define utility functions for each conversion
def file_to_db():
    print("Calling File to DB Utility....")


def file_to_output():
    print("Calling File to File Conversion Logic....")


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

    source, target = source_main[-1], target_main[-1]

    # Check the combination and call the appropriate function
    if source == target:
        print(f"This Combination is not allowed: Source and Target both are {source}")
    else:
        if source == "FILE":
            if target == "DB":
                file_to_db()
            elif target == "OUTPUT":
                file_to_output()
            elif target == "API":
                file_to_api()
        elif source == "DB":
            if target == "FILE":
                db_to_file()
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
