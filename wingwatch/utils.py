import os

from pandas import DataFrame


def write_file(data: DataFrame, write_dir: str, current_file: str, file_type='processed'):
    processed_file = rename_file(write_dir, current_file)
    print("Writing {type} file: {file}".format(type=file_type, file=processed_file))
    data.to_csv(processed_file, index=False)


def delete_file(current_file: str, file_type='processed'):
    print("Removing {type} file: {file}".format(type=file_type, file=current_file))
    os.remove(current_file)


def rename_file(write_dir, file_path):
    base_path_ext = os.path.basename(file_path)
    file_name = os.path.splitext(base_path_ext)[0]
    return "{path}/{name}.csv".format(path=write_dir, name=file_name)
