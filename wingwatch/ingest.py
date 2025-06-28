import os

import pandas as pd
from watchdog.events import (FileCreatedEvent, FileDeletedEvent,
                             FileSystemEventHandler)

import wingwatch.partition as partition
import wingwatch.transform as transform


class IngestionHandler(FileSystemEventHandler):

    def __init__(self, partition_count, write_dir, columns):
        self.partition_count = partition_count
        self.write_dir = write_dir
        self.columns = columns

    def on_created(self, event: FileCreatedEvent) -> None:
        print("Found data file to partition: {src}".format(src=event.src_path))
        data = pd.read_csv(event.src_path)
        data = data[self.columns]
        partition_ranges = partition.find_partition_ranges(len(data), self.partition_count)
        partition_files = partition.write_partitions(data, partition_ranges, self.write_dir)
        # check to see if all files have been written before removing...
        if len(partition_ranges) == len(partition_files):
            os.remove(event.src_path)

    def on_deleted(self, event: FileDeletedEvent) -> None:
        print("Data file partitioned and removed: {src}".format(src=event.src_path))


class PartitionHandler(FileSystemEventHandler):

    def __init__(self, write_dir):
        self.write_dir = write_dir

    def on_created(self, event: FileCreatedEvent) -> None:
        print("Found partition file to process: {src}".format(src=event.src_path))
        data = pd.read_csv(event.src_path)
        cleaned_data = transform.clean(data)
        transform.write_processed(cleaned_data, self.write_dir, event.src_path)
        os.remove(event.src_path)

    def on_deleted(self, event: FileDeletedEvent) -> None:
        print("Partition file processed and removed: {src}".format(src=event.src_path))
