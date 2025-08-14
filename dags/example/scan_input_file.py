from __future__ import annotations

import pendulum
import os

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def scan_files_in_directory(directory_path: str, table_name: str):
    """
    Scans a specified directory and lists all files within it.
    """
    if not os.path.isdir(directory_path):
        print(f"Error: Directory '{directory_path}' does not exist.")
        return

    print(f"Scanning directory: {directory_path}")
    postgres_conn_id = 'postgres_landdb'
    list_animal_file = []
    for root, _, files in os.walk(directory_path):
        for file in files:
            file_path = os.path.join(root, file)
            print(f"Found file: {file_path}")
            if "animal" in file_path:  
                # table_name = 'public.test_animal' 
                list_animal_file.append({"csv_filepath": file_path, "table_name": table_name,  "postgres_conn_id": postgres_conn_id})
               

    trigger_child_dag = TriggerDagRunOperator.partial(
        task_id="import_animal_csv",
        trigger_dag_id="import_animal_csv",  # Ensure this DAG exists
        # wait_for_completion=True,  # Optional: Upstream DAG waits for downstream DAG to finish
        # deferrable=True,  # Optional: Defer the waiting process to the triggerer
    ).expand(conf=list_animal_file)

    print(f"Complete process!")

with DAG(
    dag_id="scan_input_file",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Run manually or define a schedule
    catchup=False,
    tags=["file_scan", "ingestion"],
) as dag:
    scan_task = PythonOperator(
        task_id="scan_directory_task",
        python_callable=scan_files_in_directory,
        op_kwargs={"directory_path": "/home/thnghi/airflow6/ingestion/unprocess"},  # Replace with the actual directory
    )