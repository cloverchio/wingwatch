import os

import pandas as pd
from pandas import DataFrame


def write_processed(data: DataFrame, write_dir: str, current_file: str):
    processed_file = rename_file(write_dir, current_file)
    print("Writing processed file: {file}".format(file=processed_file))
    data.to_csv(processed_file, index=False)


def rename_file(write_dir, file_path):
    base_path_ext = os.path.basename(file_path)
    file_name = os.path.splitext(base_path_ext)[0]
    return "{path}/{name}.csv".format(path=write_dir, name=file_name)


def clean(data: DataFrame) -> DataFrame:
    cleaned_data = data.copy().dropna()
    convert_string_columns(cleaned_data)
    convert_datetime_columns(cleaned_data, 0.9)
    normalize_column_names(cleaned_data)
    return cleaned_data


def convert_string_columns(data: DataFrame):
    for column in data.select_dtypes(include="object").columns:
        data[column] = data[column].str.lower()


def convert_datetime_columns(data: DataFrame, threshold: float):
    for column in data.select_dtypes(include=["object", "string"]).columns:
        try:
            converted_column = pd.to_datetime(data[column], errors="coerce", format="%Y-%m-%d")
            success_ratio = converted_column.notna().mean()
            if success_ratio > threshold:
                data[column] = converted_column
        except Exception:
            continue


def normalize_column_names(data: DataFrame):
    for column in data.columns:
        normalized_column_name = column.replace(" ", "_").lower()
        data.rename(columns={column: normalized_column_name}, inplace=True)
