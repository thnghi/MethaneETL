from __future__ import annotations
import json
import pendulum 
from airflow.models.dag import DAG 
from airflow.decorators import task
from datetime import datetime
from worker.settings import Settings
from worker.pedigree_worker import PedigreeWorker
# from worker.breed_worker import BreedWorker


with DAG(
    dag_id="F2L_consumer_child_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["landing"],
) as child_dag: 
    @task
    def get_items_from_conf(**kwargs): 
        items_json = kwargs['dag_run'].conf.get('items', [])
        items = json.loads(items_json) 
        return items
    @task
    def process_item(import_operation_key, table_name, file_name, import_key):
        print(f"Processing {file_name} into {table_name} with operation {import_operation_key} and import key {import_key}")
        if table_name  == Settings.PEDIGREE_TABLE:
            worker = PedigreeWorker(file_name,import_operation_key) 
            worker.process_file()
        # elif table_name == Settings.BREED_TABLE:
        #     worker = BreedWorker(file_name,import_operation_key) 
        #     worker.import_to_landdb()

    items = get_items_from_conf()
    process_item.expand_kwargs(items)
    
     