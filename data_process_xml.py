import xml.etree.ElementTree as ET


def extract_data(filename):
    """
  Extracts name, age, and amount data from each record in an XML file.

  Args:
      filename: The path to the XML file.

  Returns:
      A list of dictionaries, where each dictionary contains the extracted data
      for a single record.
  """
    tree = ET.parse(filename)
    root = tree.getroot()

    data = []
    for record in root.findall('record'):
        record_data = {}
        record_data['name'] = record.find('name').text
        record_data['age'] = int(record.find('age').text)  # Convert age to integer
        record_data['amount'] = float(record.find('amount').text)  # Convert amount to float
        data.append(record_data)

    return data


if __name__ == "__main__":
    filename = "test_data.xml"  # Replace with the actual filename
    extracted_data = extract_data(filename)

    for record in extracted_data:
        print(f"Name: {record['name']}, Age: {record['age']}, Amount: {record['amount']}")
