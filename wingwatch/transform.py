import os

import pandas as pd
from pandas import DataFrame


def write_processed(data: DataFrame, write_dir: str, current_file: str):
    processed_file = rename_file(write_dir, current_file)
    print("Writing processed file: {file}".format(file=processed_file))
    data.to_csv(processed_file, index=False)


def clean(data: DataFrame, str_columns: list, date_columns: list) -> DataFrame:
    cleaned_data = data.dropna()
    for column in str_columns:
        cleaned_data[column].to_string()
    for column in date_columns:
        pd.to_datetime(cleaned_data[column])
    return cleaned_data


def rename_file(write_dir, file_path):
    base_path_ext = os.path.basename(file_path)
    file_name = os.path.splitext(base_path_ext)[0]
    return "{path}/{name}".format(path=write_dir, name=file_name)
