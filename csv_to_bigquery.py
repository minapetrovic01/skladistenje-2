import os
from google.cloud import bigquery
import pandas as pd

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'skladistenje2-558414757286.json'
client = bigquery.Client()
project_id = 'skladistenje2'
dataset_id = 'skladistenje2.pillow_readings'
table_id = "skladistenje2.pillow_readings.pillow"

folder_path = './pillowOutput'

dtype = {
    '_id': str,
    'snoringRange': float,
    'respirationRate': float,
    'bodyTemperature': float,
    'limbMovement': float,
    'bloodOxygen': float,
    'rem': float,
    'hoursSleeping': float,
    'heartRate': float,
    'stresState': float
}

for file_name in os.listdir(folder_path):
    if file_name.endswith('.csv'):
        file_path = os.path.join(folder_path, file_name)
        
        try:
            df = pd.read_csv(file_path,dtype=dtype)
            #df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')

            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

            client.load_table_from_dataframe(df, table_id, job_config=job_config)
            print(f"Data loaded from {file_name} into {project_id}.{dataset_id}.{table_id}")
        except pd.errors.EmptyDataError:
            print(f"The file {file_path} is empty.")
        except Exception as e:
            print(f"Error loading data from {file_name}: {e}")
