import os
from pathlib import Path

import pandas as pd
from pandas import DataFrame


def read_file(current_file: str) -> DataFrame:
    return pd.read_csv(current_file)


def read_all_files(read_dir: str) -> list:
    files = Path(read_dir).glob("*.csv")
    return [pd.read_csv(file) for file in files]


def write_file(data: DataFrame, write_dir: str, current_file: str, file_type='processed'):
    new_file = rename_file(write_dir, current_file)
    print("Writing {type} file: {file}".format(type=file_type, file=new_file))
    data.to_csv(new_file, index=False)


def delete_file(current_file: str, file_type='processed'):
    print("Removing {type} file: {file}".format(type=file_type, file=current_file))
    os.remove(current_file)


def rename_file(write_dir, file_path):
    base_path_ext = os.path.basename(file_path)
    file_name = os.path.splitext(base_path_ext)[0]
    return "{path}/{name}.csv".format(path=write_dir, name=file_name)
