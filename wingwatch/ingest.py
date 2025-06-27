import os

import pandas as pd
from watchdog.events import (FileCreatedEvent, FileDeletedEvent,
                             FileSystemEventHandler)

import wingwatch.partition as partition
import wingwatch.transform as transform


class IngestionHandler(FileSystemEventHandler):

    def __init__(self, partition_count, write_path, columns):
        self.partition_count = partition_count
        self.write_path = write_path
        self.columns = columns

    def on_created(self, event: FileCreatedEvent) -> None:
        print("Found data file to partition: {src_path}".format(
            src_path=event.src_path))
        data = pd.read_csv(event.src_path)
        data = data[self.columns]
        partition_ranges = partition.find_partition_ranges(
            len(data), self.partition_count)
        print(partition_ranges)
        partition_files = partition.write_partitions(
            data, partition_ranges, self.write_path)
        # check to see if all files have been written before removing...
        if len(partition_ranges) == len(partition_files):
            os.remove(event.src_path)

    def on_deleted(self, event: FileDeletedEvent) -> None:
        print("Data file partitioned and removed: {src_path}".format(
            src_path=event.src_path))


class PartitionHandler(FileSystemEventHandler):

    def __init__(self, write_directory, str_columns, date_columns):
        self.write_directory = write_directory
        self.str_columns = str_columns
        self.date_columns = date_columns

    def on_created(self, event: FileCreatedEvent) -> None:
        print("Found partition file to process: {src_path}".format(
            src_path=event.src_path))
        data = pd.read_csv(event.src_path)
        cleaned_data = transform.transform(
            data, self.str_columns, self.date_columns)

    def on_deleted(self, event: FileDeletedEvent) -> None:
        print("Partition file processed and removed: {src_path}".format(
            src_path=event.src_path))
