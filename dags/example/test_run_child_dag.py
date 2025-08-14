from __future__ import annotations

import pendulum
import os
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.dag import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="parent_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:

    @task
    def get_items_to_process():
        # This task could fetch data from a database, API, etc.

        # if not os.path.isdir(directory_path):
        #     print(f"Error: Directory '{directory_path}' does not exist.")
        # return 
        directory_path = "/home/thnghi/airflow6/ingestion/unprocess"
        table_name = "public.test_animal"
        print(f"Scanning directory: {directory_path}") 
        list_animal_file = []
        for root, _, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                print(f"Found file: {file_path}")
                if "animal" in file_path:  
                    # table_name = 'public.test_animal' 
                    list_animal_file.append({"csv_filepath": file_path, "table_name": table_name })
                    
        return list_animal_file

    trigger_child_dag = TriggerDagRunOperator.partial(
        task_id="trigger_child_dag",
        trigger_dag_id="child_dag",  # Ensure this DAG exists
    ).expand(conf=get_items_to_process())

# Define the child_dag (in a separate file or within the same file for simplicity)
with DAG(
    dag_id="child_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example"],
) as child_dag:

    @task
    def process_item(**kwargs):
        
        item_data = kwargs["dag_run"].conf
        postgres_conn_id = 'postgres_landdb'
        print(f"Processing item: {item_data}")
        csv_filepath = item_data["csv_filepath"]
        table_name = item_data["table_name"]
 
        pg_hook_landing = PostgresHook(postgres_conn_id=postgres_conn_id)
        pg_hook_landing.copy_expert(
        f"COPY {table_name} (ANIMAL,NAME,DOB,SIRE,NM_PTA)  FROM STDIN WITH CSV HEADER",
        csv_filepath
    ) 
    process_item()

 