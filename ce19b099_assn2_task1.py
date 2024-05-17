#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
import random

# Define the base URL and year
BASE_URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/"
YEAR = "YYYY"  # Replace YYYY with the desired year

def fetch_page():
    # Task to fetch the page containing the location wise datasets
    command = f"wget {BASE_URL}{YEAR}"
    os.system(command)

def select_random_files(num_files):
    # Task to select random data files
    files = os.listdir(YEAR)
    selected_files = random.sample(files, num_files)
    return selected_files

def fetch_individual_files(file):
    # Task to fetch individual data files
    command = f"wget {BASE_URL}{YEAR}/{file}"
    os.system(command)

def zip_files(files):
    # Task to zip the selected files into an archive
    command = f"zip csv_archive.zip {' '.join(files)}"
    os.system(command)

def move_archive():
    # Task to move the archive to a required location
    command = f"mv csv_archive.zip /home/aravind/Documents/csv_archive.zip"
    os.system(command)

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 2),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'noaa_data_pipeline',
    default_args=default_args,
    description='A simple DAG to fetch and process NOAA data',
    schedule_interval=None,
)

# Define tasks
fetch_page_task = BashOperator(
    task_id='fetch_page',
    bash_command=fetch_page,
    dag=dag,
)

select_files_task = PythonOperator(
    task_id='select_files',
    python_callable=select_random_files,
    op_kwargs={'num_files': 5},  # Change the number as required
    dag=dag,
)

fetch_files_tasks = []
for i in range(5):  # Change the number to match the number of selected files
    task = BashOperator(
        task_id=f'fetch_file_{i}',
        bash_command=fetch_individual_files,
        env={'file': "{{ task_instance.xcom_pull(task_ids='select_files')[%d] }}" % i},
        dag=dag,
    )
    fetch_files_tasks.append(task)

zip_files_task = PythonOperator(
    task_id='zip_files',
    python_callable=zip_files,
    op_kwargs={'files': "{{ task_instance.xcom_pull(task_ids='select_files') }}"},
    dag=dag,
)

move_archive_task = BashOperator(
    task_id='move_archive',
    bash_command=move_archive,
    dag=dag,
)

# Define task dependencies
fetch_page_task >> select_files_task
select_files_task >> fetch_files_tasks
fetch_files_tasks >> zip_files_task
zip_files_task >> move_archive_task

