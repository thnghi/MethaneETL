from __future__ import annotations
import pendulum
import os
from airflow.models.dag import DAG
from airflow.api.common.experimental.trigger_dag import trigger_dag 
from airflow.operators.python_operator import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import task  

from datetime import datetime
from worker.settings import Settings
from worker.common import *
from worker.master_run_worker import MasterRunWorker
from worker.user_run_worker import UserRunWorker
import json
import uuid
from airflow.hooks.postgres_hook import PostgresHook

pg_hook_landing = PostgresHook(postgres_conn_id=Settings.CONN_LANDDB)

def get_list_operation(**kwargs): 
 
    run_worker = UserRunWorker(pg_hook_landing)  
    list_operation = run_worker.get_list_import_operation_to_be_run()
    list_import_key  =  ','.join(str(item) for item in {item[3]  for item in list_operation})  
    if list_import_key != '':
        run_worker.start_import_run(list_import_key)  
    list_operation_pedigree_json = task_get_list_operation_per_type("pedigree",Settings.PEDIGREE_TABLE,list_operation)
    list_operation_calving_json = task_get_list_operation_per_type("calving",Settings.CALVING_TABLE,list_operation)
    
    print(f"list_import_key: {list_import_key}")
    kwargs['ti'].xcom_push(key='list_import_key', value=list_import_key)
    kwargs['ti'].xcom_push(key='list_operation_pedigree', value=list_operation_pedigree_json)
    kwargs['ti'].xcom_push(key='list_operation_calving', value=list_operation_calving_json)
    

def task_get_list_operation_per_type(ip_file_type, table_name, list_operation): 
    list_file = []  
    for file in list_operation:
        import_operation_key = file[0]
        file_type = file[1]
        file_name = file[2] 
        import_key = file[3] 
        if file_type.lower() == ip_file_type:
            list_file.append({"import_operation_key":import_operation_key, "table_name":table_name, "file_name":file_name,"import_key":import_key }) 

    json_string = json.dumps(list_file)  

    return json_string
 

# def trigger_child_dag(dag_id, items, **kwargs):
def trigger_child_dag(dag_id,type, items):
    unique_run_id = f"{dag_id}_{type}__{uuid.uuid4()}"
    unique_execution_date = pendulum.now("UTC") 
    print(f"unique_run_id: {unique_run_id}")
    trigger_dag(
        dag_id=dag_id,
        conf={"items": items},
        execution_date=unique_execution_date, #kwargs['execution_date'],
        run_id=unique_run_id, 
    )

    wait_for_dag_run_complete(dag_id,unique_run_id) 
 
def finish_run(items):  
    print(f"Finish process for import keys: {items}")
    run_worker = UserRunWorker(pg_hook_landing)  
    run_worker.finish_import_run(items)   
    

with DAG(
    dag_id="F2L_consumer_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    # schedule_interval = '*/2 * * * *',
    catchup=False,
    tags=["landing"],
) as dag:  
    
    get_list_operation = PythonOperator(
        task_id='task_get_list_operation',
        python_callable=get_list_operation,
        provide_context=True,
    )

    trigger_kind_pedigree = PythonOperator(
        task_id='trigger_consumer_child_dag_pedigree',
        python_callable=trigger_child_dag,
        op_kwargs={
            'dag_id': 'F2L_consumer_child_dag',
            'type':"pedigree",
            'items': "{{ ti.xcom_pull(task_ids='task_get_list_operation', key='list_operation_pedigree') }}"
        },
        provide_context=True,
    )

    trigger_kind_calving = PythonOperator(
        task_id='trigger_consumer_child_dag_calving',
        python_callable=trigger_child_dag,
        op_kwargs={
            'dag_id': 'F2L_consumer_child_dag',
            'type':"calving",
            'items': "{{ ti.xcom_pull(task_ids='task_get_list_operation', key='list_operation_calving') }}"
        },
        provide_context=True,
    )

    # trigger_kind_pedigree = TriggerDagRunOperator(
    #     task_id="trigger_consumer_child_dag_pedigree",
    #     trigger_dag_id="F2L_consumer_child_dag",
    #     conf={"items": "{{ ti.xcom_pull(task_ids='task_get_list_operation', key='list_operation_pedigree') }}"}, 
    #     wait_for_completion=True,
    #     allowed_states=["success"],
    #     failed_states=["failed"],
    #     fail_when_dag_is_paused=False,
    #     reset_dag_run=True,
    #     poke_interval=60,
    #     skip_when_already_exists=False,
    #     logical_date=datetime.utcnow(),  # Optional but recommended
    #     dag=dag
    # )
 
    finish_run = PythonOperator(
        task_id='task_finish_run',
        python_callable=finish_run,
        op_kwargs={ 
            'items': "{{ ti.xcom_pull(task_ids='task_get_list_operation', key='list_import_key') }}"
        },
        provide_context=True,
    )

     
    # get_list_operation >> [trigger_kind_pedigree,trigger_kind_calving] 
    get_list_operation >> trigger_kind_pedigree >> trigger_kind_calving >> finish_run