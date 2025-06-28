from pandas import DataFrame


def groupby(data: DataFrame, columns: list, index_name=None) -> DataFrame:
    return data.groupby(columns).size().reset_index(name=index_name)


def count(data: DataFrame, column: str, row_count=None, index_name=None) -> DataFrame:
    return (data[column].value_counts()
            .head(row_count)
            .reset_index(name=index_name)
            .rename(columns={'index': column}))
