from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def fetch_and_process_data(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id="postgres_landdb")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM public.test_airflow;")
    data = cursor.fetchall()
    cursor.close()
    conn.close()

    # Process data (e.g., transform, filter, push to XCom)
    kwargs['ti'].xcom_push(key='fetched_data', value=data)

with DAG(
    dag_id='load_postgres_table2',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None, # Or define your desired schedule
    catchup=False,
    tags=['csv', 'data_ingestion'],
) as dag:
    fetch_data_task = PythonOperator(
        task_id="fetch_and_process_data",
        python_callable=fetch_and_process_data,
        provide_context=True,
    )
