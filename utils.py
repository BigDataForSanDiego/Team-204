import xml.etree.ElementTree as ET
import pandas as pd
import datetime as dt

def clean_type(df, _type):
    name = _type
    try:
        unit = df[df['type'] == _type]['unit'].iloc[0]
    except:
        raise ValueError(f"{_type} does not have a unit")
    name = name.replace('HKQuantityTypeIdentifier', '').replace('HKCategoryTypeIdentifier', '')
    i = 1
    while i < len(name):
        if name[i].isupper() and name[i-1].islower():
            name = name[:i] + ' ' + name[i:]
            i += 1
        i += 1
    name += f' ({unit})'
    return name.strip()

def clean_types(df):
    mapping = {
        _type: clean_type(df, _type) for _type in df['type'].unique()
    }
    return df.assign(type=df['type'].replace(mapping))

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


def parse_csv(filepath):
    df = pd.read_csv(filepath, parse_dates=['startDate', 'endDate', 'creationDate'])
    return df


def ts_type(df, _type, resample='1D', resample_agg='sum'):
    if _type not in df['type'].unique():
        # print(df['type'].unique())
        raise ValueError(f"{_type} not found in dataframe")
    _df = df[df['type'] == _type].set_index('startDate')['value'].rename(_type)
    return _df.resample(resample).agg(resample_agg)


if __name__ == '__main__':
    # df = parse_xml('data/carter_export.xml')
    # df.to_csv('data/carter_export.csv', index=False)
    df = parse_csv('data/carter_export_2024_jan.csv')
    # df = clean_types(df)
    # df.to_csv('data/carter_export_2024_jan.csv', index=False)
    df = clean_types(df)
    df.to_csv('data/carter_export_2024_jan_clean.csv', index=False)
    # df[(df['startDate'] >= '2024-01-01') & (df['startDate'] <= '2024-01-31')].to_csv('data/carter_export_2024_jan.csv', index=False)
    # print(ts_type(df, 'ActiveEnergyBurned'))
    # print(df.shape[0])
    # print(df['type'].unique())
    # print(ts_type(df, 'HKQuantityTypeIdentifierActiveEnergyBurned'))