import xml.etree.ElementTree as ET
import pandas as pd
import datetime as dt

def parse_xml(filepath):

    tree = ET.parse(filepath) 

    root = tree.getroot()
    record_list = [x.attrib for x in root.iter('Record')]

    record_data = (
        pd.DataFrame(record_list)
        .assign(
        creationDate=lambda x: pd.to_datetime(x['creationDate']),
        startDate=lambda x: pd.to_datetime(x['startDate']),
        endDate=lambda x: pd.to_datetime(x['endDate']),
        value=lambda x: pd.to_numeric(x['value'], errors='coerce')
        )
    )

    return record_data

if __name__ == '__main__':
    # df = parse_xml('data/carter_export.xml')
    # df.to_csv('data/carter_export.csv', index=False)
    df = pd.read_csv('data/carter_export.csv')
    df[df['startDate'] > '2024-01-01'].to_csv('data/carter_export_2024.csv', index=False)