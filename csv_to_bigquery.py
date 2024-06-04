import os
from google.cloud import bigquery
import pandas as pd

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'skladistenje2-558414757286.json'
client = bigquery.Client()
project_id = 'ipz-vezba4-421011'
dataset_id = 'skladistenje2.pillow_readings'
table_id = "ipz-vezba4-421011.garden_readings.readings"

folder_path = '/path/to/csv/folder/'

for file_name in os.listdir(folder_path):
    if file_name.endswith('.csv'):
        file_path = os.path.join(folder_path, file_name)
        df = pd.read_csv(file_path,skiprows=1)
        #df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')

        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

        client.load_table_from_dataframe(df, table_id, job_config=job_config)
        print(f"Data loaded from {file_name} into {project_id}.{dataset_id}.{table_id}")
