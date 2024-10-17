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


def parse_csv(filepath):
    df = pd.read_csv(filepath, parse_dates=['startDate', 'endDate', 'creationDate'])
    return df


def ts_type(df, _type, resample='1D', resample_agg='sum'):
    def clean_name():
        name = _type
        unit = df[df['type'] == _type]['unit'].iloc[0]
        name = name.replace('HKQuantityTypeIdentifier', '').replace('HKCategoryTypeIdentifier', '')
        for c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
            name = name.replace(c, ' ' + c)
        name += f' ({unit})'
        return name.strip()
    _df = df[df['type'] == _type].set_index('startDate')['value'].rename(clean_name())
    return _df.resample(resample).agg(resample_agg)


if __name__ == '__main__':
    # df = parse_xml('data/carter_export.xml')
    # df.to_csv('data/carter_export.csv', index=False)
    df = parse_csv('data/carter_export_2024_jan.csv')
    # df[(df['startDate'] >= '2024-01-01') & (df['startDate'] <= '2024-01-31')].to_csv('data/carter_export_2024_jan.csv', index=False)
    # print(ts_type(df, 'ActiveEnergyBurned'))
    # print(df.shape[0])
    # print(df['type'].unique())
    print(ts_type(df, 'HKQuantityTypeIdentifierActiveEnergyBurned'))