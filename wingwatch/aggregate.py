import pandas as pd
from pandas import DataFrame


def reduce(partition_data: list, columns: list, index_name=None) -> DataFrame:
    merged = pd.concat(partition_data, ignore_index=True)
    return groupby(merged, columns, index_name)


def groupby(data: DataFrame, columns: list, index_name=None) -> DataFrame:
    columns = [column.lower() for column in columns]
    return data.groupby(columns).size().reset_index(name=index_name)


def count(data: DataFrame, column: str, row_count=None, index_name=None) -> DataFrame:
    return (data[column].value_counts()
            .head(row_count)
            .reset_index(name=index_name)
            .rename(columns={'index': column}))
