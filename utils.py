import xml.etree.ElementTree as ET
import pandas as pd
import dask.dataframe as dd
import datetime as dt

import logging
import time

def clean_type(df, _type):
    name = str(_type)
    try:
        unit = df[df['type'] == name]['unit'].astype(str).dropna().compute().iloc[0]
        logging.info(f"Detected unit: {unit} for type: {_type}")
    except Exception as e:
        logging.info(f"Cannot detect unit for type: {_type}")
        logging.debug(e)
        unit = 'NO_UNIT'
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
    return df.assign(type=df['type'].cat.rename_categories(mapping))

def parse_xml(filepath):

    tree = ET.parse(filepath) 

    root = tree.getroot()
    record_list = [x.attrib for x in root.iter('Record')]

    df = (
        dd.DataFrame(record_list)
        .assign(
        creationDate=lambda x: dd.to_datetime(x['creationDate']),
        startDate=lambda x: dd.to_datetime(x['startDate']),
        endDate=lambda x: dd.to_datetime(x['endDate']),
        value=lambda x: dd.to_numeric(x['value'], errors='coerce')
        )
    )

    dtype={'type': 'category', 'sourceName': 'category', 'sourceVersion': 'category', 'unit': 'category', 'value': 'float64', 'device': 'str'}

    return df.astype(dtype)


def parse_csv(filepath):
    df = dd.read_csv('data/carter_export.csv', dtype={'type': 'category', 'sourceName': 'category', 'sourceVersion': 'category', 'unit': 'category', 'value': 'float64', 'device': 'str'})
    df = df.assign(
        creationDate=lambda x: dd.to_datetime(x['creationDate']),
        startDate=lambda x: dd.to_datetime(x['startDate']),
        endDate=lambda x: dd.to_datetime(x['endDate']),
    )
    return df


def ts_type(df, _type, resample='1D', resample_agg='sum'):
    try:
        _df = df[df['type'] == _type].set_index('startDate')['value'].rename(_type)
        return _df.resample(resample).agg(resample_agg).compute()
    except Exception as e:
        logging.debug(e)
        logging.info(f"{_type} not found in dataframe")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    logging.info("Parsing CSV file")
    start_time = time.time()
    df = parse_csv('data/carter_export.csv')
    end_time = time.time()
    logging.info(f"Time taken to parse CSV: {end_time - start_time} seconds")

    logging.info("Cleaning types")
    start_time = time.time()
    df = clean_types(df)
    end_time = time.time()
    logging.info(f"Time taken to clean types: {end_time - start_time} seconds")

    logging.info("Writing to parquet")
    start_time = time.time()
    df.to_parquet('data/carter_export.parquet')
    end_time = time.time()
    logging.info(f"Time taken to write to parquet: {end_time - start_time} seconds")

    logging.info("Reading from parquet")
    start_time = time.time()
    df = dd.read_parquet('data/carter_export.parquet')
    end_time = time.time()
    logging.info(f"Time taken to read from parquet: {end_time - start_time} seconds")