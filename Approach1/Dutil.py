import pandas as pd
import json
import argparse
import xml.etree.ElementTree as ET


def extract_csv(file_path, delimiter):
    return pd.read_csv(file_path, delimiter=delimiter)


def extract_txt(file_path, delimiter):
    with open(file_path, 'r') as file:
        data = [line.strip().split(delimiter) for line in file.readlines()]
    headers = data[0]
    rows = data[1:]
    return pd.DataFrame(rows, columns=headers)


def parse_xml_to_dict(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    data = []
    for book in root.findall('book'):
        book_data = {
            'id': book.get('id'),
            'author': book.find('author').text,
            'title': book.find('title').text,
            'genre': book.find('genre').text,
            'price': float(book.find('price').text),
            'publish_date': book.find('publish_date').text,
            'description': book.find('description').text,
        }
        data.append(book_data)
    return data


def extract_xml(file_path):
    books_data = parse_xml_to_dict(file_path)
    return pd.DataFrame(books_data)


def transform_data(data_frame):
    # Adjust the columns dynamically based on the input data
    required_columns = {'name', 'age', 'date_of_joining', 'id', 'author', 'title', 'genre', 'price', 'publish_date'}
    columns_to_keep = [col for col in data_frame.columns if col in required_columns]
    return data_frame[columns_to_keep]


def load_data(data_frame, output_path, output_format, delimiter=None):
    if output_format == 'csv':
        data_frame.to_csv(output_path, index=False, sep=delimiter)
    elif output_format == 'txt':
        data_frame.to_csv(output_path, index=False, sep=delimiter)
    elif output_format == 'json':
        data_frame.to_csv(output_path, index=False, sep=delimiter)
    else:
        raise ValueError(f"Unsupported output format: {output_format}")


def process_file(file_type, config):
    input_path = config[file_type]['input_path']
    output_path = config[file_type]['output_path']
    delimiter = config[file_type].get('delimiter', ',')

    if file_type == 'csv':
        data = extract_csv(input_path, delimiter)
    elif file_type == 'txt':
        data = extract_txt(input_path, delimiter)
    elif file_type == 'xml':
        data = extract_xml(input_path)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    transformed_data = transform_data(data)
    output_format = output_path.split('.')[-1]
    load_data(transformed_data, output_path, output_format, delimiter)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ETL Utility')
    parser.add_argument('file_type', type=str, help='Type of the file to process (csv, txt, xml)')
    parser.add_argument('config_path', type=str, help='Path to the configuration JSON file')

    args = parser.parse_args()

    with open(args.config_path, 'r') as config_file:
        config = json.load(config_file)

    process_file(args.file_type, config)
