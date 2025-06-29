import time

from watchdog.observers import Observer

from wingwatch.ingest import RawDataHandler, PartitionDataHandler, ProcessedDataHandler

if __name__ == '__main__':
    columns = ['AIRPORT_ID', 'AIRPORT', 'INCIDENT_DATE', 'INCIDENT_MONTH', 'INCIDENT_YEAR', 'TIME', 'TIME_OF_DAY',
               'SPECIES']

    raw_data_handler = RawDataHandler(columns, "../data/partitions", 4)
    partition_data_handler = PartitionDataHandler("../data/processed")
    processed_data_handler = ProcessedDataHandler(["INCIDENT_YEAR"], "strike_count", "../data/aggregates")

    observer = Observer()

    observer.schedule(raw_data_handler, "../data/raw", recursive=True)
    observer.schedule(partition_data_handler, "../data/partitions", recursive=True)
    observer.schedule(processed_data_handler, "../data/processed", recursive=True)

    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
