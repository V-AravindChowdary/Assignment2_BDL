import os
import zipfile
from datetime import datetime, timedelta
import apache_beam as beam
import pandas as pd
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.runners.interactive.interactive_beam as ib
import shutil
import json

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'data_processing_pipeline',
    default_args=default_args,
    description='DAG for processing and visualizing data using Apache Beam',
    schedule_interval='*/1 * * * *',  # Run every minute
)

# Task 1: Sensor to check for the zip file
file_sensor_task = FileSensor(
    task_id='file_sensor_task',
    poke_interval=5,
    timeout=10,
    filepath='/home/aravind/Documents/csv_archive.zip',
    mode='poke',
    dag=dag,
)

# Task 2: BashOperator to validate and unzip the archive
unzip_task = BashOperator(
    task_id='unzip_task',
    bash_command='''
        if [ ! -d "/home/aravind/Documents/data_directory" ]; then
            unzip -t /home/aravind/Documents/csv_archive.zip
            unzip /home/aravind/Documents/csv_archive.zip -d /home/aravind/Documents/data_directory
        fi
    ''',
    dag=dag,
)

# Task 3: Function to process CSV files
def parse_csv(csv_file):
    df = pd.read_csv(csv_file)
    data_tuples = []
    cols_to_analyze = ['HourlyDryBulbTemperature', 'HourlyWindSpeed', 'HourlyDewPointTemperature', 'HourlyPressureChange', 'HourlyRelativeHumidity']
    relevant_cols = ['LATITUDE', 'LONGITUDE'] + cols_to_analyze
    df[relevant_cols] = df[relevant_cols].apply(pd.to_numeric, errors='coerce')
    df.dropna(inplace=True)
    for _, row in df.iterrows():
        lat = row['LATITUDE']
        lon = row['LONGITUDE']
        month = pd.to_datetime(row['DATE']).month
        temp = row['HourlyDryBulbTemperature']
        wind = row['HourlyWindSpeed']
        dptemp = row['HourlyDewPointTemperature']
        hpc = row['HourlyPressureChange']
        hrh = row['HourlyRelativeHumidity']
        data_tuples.append((month, float(lat), float(lon), float(temp), float(wind),float(dptemp),float(hpc),float(hrh)))
    return data_tuples

# Task 4: Function to convert CSV files into the desired format
def convert_csv_to_tuples(directory_path):
    with beam.Pipeline(runner='DirectRunner') as pipeline:
        csv_files = [os.path.join('/home/aravind/Documents/data_directory/', f) for f in os.listdir('/home/aravind/Documents/data_directory/') if f.endswith('.csv')]
        processed_data = (
            pipeline 
            | beam.Create(csv_files)
            | beam.Map(parse_csv)
            | beam.Map(compute_monthly_avg)
            | beam.io.WriteToText('/home/aravind/Documents/processed_data.txt')
        )
text_file_path = '/home/aravind/Documents/processed_data.txt-00000-of-00001'

# Function to compute monthly averages
def compute_monthly_avg(data):
    df = pd.DataFrame(data, columns=['Month', 'Latitude', 'Longitude', 'Temperature', 'WindSpeed', 'DPtemperature', 'Pressure', 'Relativehumidity'])
    df.dropna(inplace=True)
    results = []
    grouped_df = df.groupby(['Month', 'Latitude', 'Longitude']).agg({'Temperature': 'mean', 'WindSpeed': 'mean', 'DPtemperature': 'mean','Pressure': 'mean', 'Relativehumidity': 'mean'}).reset_index()
    for _, row in grouped_df.iterrows():
        results.append([row['Latitude'], row['Longitude'], row['Month'], row['Temperature'], row['WindSpeed'],row['DPtemperature'], row['Pressure'],row['Relativehumidity']])
    return json.dumps(results)

# Define the PythonOperator
process_data_task = PythonOperator(
    task_id='process_data_task',
    python_callable=convert_csv_to_tuples,
    op_kwargs={"directory_path": '/home/aravind/Documents/data_directory/'},
    dag=dag,
)

# Task 5: BashOperator to clean up the files
cleanup_task = BashOperator(
    task_id='cleanup_task',
    bash_command='rm -rf /home/aravind/Documents/data_directory/*',
    dag=dag,
)

# Set the task dependencies
file_sensor_task >> unzip_task >> process_data_task >> cleanup_task

