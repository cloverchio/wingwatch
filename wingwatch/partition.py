import uuid

from pandas import DataFrame


def write_partitions(data: DataFrame, partition_ranges: list, write_dir: str) -> list:
    partition_index = 1
    partition_files = []
    file_id = str(uuid.uuid4())
    for starting_offset, ending_offset in partition_ranges:
        partition_data = data.iloc[starting_offset:ending_offset]
        partition_file = "{dir}/{id}_{index}.csv".format(dir=write_dir, id=file_id, index=partition_index)
        print("Writing partition file: {file_path}".format(file_path=partition_file))
        partition_data.to_csv(partition_file, index=False)
        partition_files.append(partition_file)
        partition_index += 1
    return partition_files


def find_partition_ranges(data_size, partition_count):
    partition_size, remainder = divmod(data_size, partition_count)
    partition_ranges = []
    starting_offset = 0
    for i in range(partition_count):
        ending_offset = starting_offset + partition_size + (1 if i < remainder else 0)
        partition_ranges.append((starting_offset, ending_offset))
        starting_offset = ending_offset
    return partition_ranges
