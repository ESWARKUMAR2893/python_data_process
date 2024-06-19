import argparse
import json
import os

def load_json(file_name):
    with open(file_name, 'r') as file:
        return json.load(file)


def read_database_config(filename, number):
    config = load_json(filename)
    db_type = config[str(number)]["type"]
    print("ReadDB config...",db_type)
    return db_type


def read_file_config(filename, number):
    config = load_json(filename)
    # Access the file name for the given number
    file_info = config[str(number)]
    # Get the file extension without the leading dot
    file_extension = os.path.splitext(file_info)[1][1:]
    print("Read File Config ",file_info)
    return file_info, file_extension


def process_file(file, num):
    if 'DbConfig' in file:
        return read_database_config(file, num)
    elif 'FileConfig' in file:
        return read_file_config(file, num)
    else:
        print(f"Unknown configuration type for {file}")


def main():
    parser = argparse.ArgumentParser(description='Read database type and file info from JSON config files.')

    parser.add_argument('file1', type=str, help='The first JSON configuration file')
    parser.add_argument('num1', type=int, help='The key number to access in the first JSON file')
    parser.add_argument('file2', type=str, help='The second JSON configuration file')
    parser.add_argument('num2', type=int, help='The key number to access in the second JSON file')

    args = parser.parse_args()

    source = process_file(args.file1, args.num1)
    target = process_file(args.file2, args.num2)
    print("Source:", source, " Target:", target)


if __name__ == '__main__':
    main()
