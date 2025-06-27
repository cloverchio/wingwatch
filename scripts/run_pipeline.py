import time

from watchdog.observers import Observer

from wingwatch.ingest import IngestionHandler, PartitionHandler

if __name__ == '__main__':
    columns = ['AIRPORT_ID', 'AIRPORT', 'INCIDENT_DATE',
               'INCIDENT_MONTH', 'INCIDENT_YEAR', 'TIME', 'TIME_OF_DAY', 'SPECIES']
    ingestion_handler = IngestionHandler(
        4, "../data/partitions/wingwatch", columns)
    partition_handler = PartitionHandler("../data/processed/wingwatch",
                                         ['AIRPORT_ID', 'AIRPORT', 'INCIDENT_MONTH', 'INCIDENT_YEAR', 'TIME_OF_DAY',
                                          'SPECIES'], ['INCIDENT_DATE'])
    ingestion_observer = Observer()
    partition_observer = Observer()
    partition_observer.schedule(
        ingestion_handler, "../data/raw/", recursive=True)
    partition_observer.schedule(
        partition_handler, "../data/partitions/", recursive=True)
    ingestion_observer.start()
    partition_observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        ingestion_observer.stop()
        partition_observer.stop()
    ingestion_observer.join()
    partition_observer.join()
