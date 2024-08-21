import pyarrow.parquet as pq
import os
import logging

logger = logging.getLogger(__name__)

def repartition_parquet(file_name, year, month, no_of_partitions, input_dir='../../raw_data', output_dir='../../repartition'):
    table = pq.read_table(f'{input_dir}/{year}/{month}/{file_name}')
    chunk_size = table.num_rows // no_of_partitions

    tables = []
    for i in range(no_of_partitions):
        if i < no_of_partitions - 1:
            tables.append(table.slice(i * chunk_size, chunk_size))
        else:
            tables.append(table.slice(i * chunk_size, chunk_size + (table.num_rows % no_of_partitions)))

    logger.info(f'Repartition Start: {file_name}')
    for i, partition_table in enumerate(tables):
        output_folder_path = f'{output_dir}/{year}/{month}'
        output_file_name = f'repartition-{i}-yellow-taxi-data-{year}-{month}.parquet'
        os.makedirs(output_folder_path, exist_ok=True)

        output_file_path = f'{output_folder_path}/{output_file_name}'
        pq.write_table(partition_table, output_file_path)
    logger.info(f'Repartition Complete: {file_name}')