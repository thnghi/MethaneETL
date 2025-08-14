from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime

def import_csv_to_postgres(csv_filepath, table_name, postgres_conn_id):
    pg_hook_landing = PostgresHook(postgres_conn_id=postgres_conn_id)
    pg_hook_landing.copy_expert(
    f"COPY {table_name} (ANIMAL,NAME,DOB,SIRE,NM_PTA)  FROM STDIN WITH CSV HEADER",
    csv_filepath
) 

with DAG(
    dag_id='import_animal_csv',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    import_task = PythonOperator(
        task_id='import_animal_csv',
        python_callable=import_csv_to_postgres,
        op_kwargs={
            'csv_filepath': '/home/thnghi/airflow6/input/input_test_animal_6MB.csv',  # Replace with actual path
            'table_name': 'public.test_animal',  # Replace with your table name
            'postgres_conn_id': 'postgres_landdb'  # Your Airflow PostgreSQL connection ID
        }
    )