import pandas as pd
from pandas import DataFrame


def clean(data: DataFrame) -> DataFrame:
    return (data.copy()
            .dropna()
            .pipe(convert_string_columns)
            .pipe(convert_datetime_columns, threshold=0.9)
            .pipe(normalize_column_names))


def convert_string_columns(data: DataFrame) -> DataFrame:
    for column in data.select_dtypes(include="object").columns:
        data[column] = data[column].str.lower()
    return data


def convert_datetime_columns(data: DataFrame, threshold: float) -> DataFrame:
    for column in data.select_dtypes(include=["object", "string"]).columns:
        try:
            converted_column = pd.to_datetime(data[column], errors="coerce", format="%Y-%m-%d")
            success_ratio = converted_column.notna().mean()
            if success_ratio > threshold:
                data[column] = converted_column
        except Exception:
            continue
    return data


def normalize_column_names(data: DataFrame) -> DataFrame:
    for column in data.columns:
        normalized_column_name = column.replace(" ", "_").lower()
        data.rename(columns={column: normalized_column_name}, inplace=True)
    return data
