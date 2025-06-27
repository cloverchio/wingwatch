import pandas as pd
from pandas import DataFrame


def write_processed(data: DataFrame, src_file: str, write_path: str) -> list:
    print(write_path)
    print(src_file)


def transform(data: DataFrame, str_columns: list, date_columns: list) -> DataFrame:
    cleaned_data = data.dropna()
    for column in str_columns:
        cleaned_data[column].to_string()
    for column in date_columns:
        pd.to_datetime(cleaned_data[column])
    return cleaned_data
