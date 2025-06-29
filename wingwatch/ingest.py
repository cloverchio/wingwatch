import threading
from queue import Queue

from watchdog.events import (FileCreatedEvent, FileDeletedEvent,
                             FileSystemEventHandler)

import wingwatch.aggregate as aggregate
import wingwatch.partition as partition
import wingwatch.transform as transform
import wingwatch.utils as utils


class AsyncEventHandler(FileSystemEventHandler):

    def __init__(self, processing_func, removal_func):
        self.event_queue = Queue()
        self.processing_func = processing_func
        self.removal_func = removal_func
        self.processed_files = set()
        self.worker = threading.Thread(target=self._worker, daemon=True)
        self.worker.start()

    def on_created(self, event: FileCreatedEvent) -> None:
        if event.src_path in self.processed_files:
            return
        self.processed_files.add(event.src_path)
        print("Queueing event for: {event}".format(event=event.src_path))
        self.event_queue.put(event.src_path)

    def on_deleted(self, event: FileDeletedEvent) -> None:
        self.removal_func(event.src_path)

    def _worker(self):
        while True:
            event = self.event_queue.get()
            self.processing_func(event)


class RawDataHandler(AsyncEventHandler):

    def __init__(self, columns, write_dir, partition_count):
        self.columns = columns
        self.write_dir = write_dir
        self.partition_count = partition_count
        super().__init__(processing_func=self._process, removal_func=self._remove)

    def _process(self, event):
        print("Found data file to partition: {src}".format(src=event))
        data = utils.read_file(event)
        data = data[self.columns]
        partition_ranges = partition.find_partition_ranges(len(data), self.partition_count)
        partition_files = partition.write_partitions(data, partition_ranges, self.write_dir)
        # check to see if all files have been written before removing...
        if len(partition_ranges) == len(partition_files):
            utils.delete_file(event)

    @staticmethod
    def _remove(event):
        print("Data file partitioned and removed: {src}".format(src=event))


class PartitionDataHandler(AsyncEventHandler):

    def __init__(self, write_dir):
        self.write_dir = write_dir
        super().__init__(processing_func=self._process, removal_func=self._remove)

    def _process(self, event):
        print("Found partition file to process: {src}".format(src=event))
        data = utils.read_file(event)
        cleaned_data = transform.clean(data)
        utils.write_file(cleaned_data, self.write_dir, event)
        utils.delete_file(event)

    @staticmethod
    def _remove(event):
        print("Partition file processed and removed: {src}".format(src=event))


class ProcessedDataHandler(AsyncEventHandler):

    def __init__(self, groupby_columns, name, write_dir):
        self.groupby_columns = groupby_columns
        self.name = name
        self.write_dir = write_dir
        super().__init__(processing_func=self._process, removal_func=self._remove)

    def _process(self, event):
        print("Found processed file to aggregate: {src}".format(src=event))
        partition_data = utils.read_file(event)
        partition_groupby = aggregate.groupby(partition_data, self.groupby_columns, self.name)
        utils.write_file(partition_groupby, self.write_dir, event)
        utils.delete_file(event)

    @staticmethod
    def _remove(event):
        print("Processed file aggregated and removed: {src}".format(src=event))
