from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import csv

def export_postgres_to_csv(table_name, output_filepath,postgres_conn_id):
    pg_hook_landing = PostgresHook(postgres_conn_id=postgres_conn_id)
    sql_query = f"SELECT * FROM {table_name};" # Or a more complex query
    
    # Execute the query and fetch results
    records = pg_hook_landing.get_records(sql_query)
    with open(output_filepath, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        # Optionally write header row if needed (fetch column names from cursor description)
        # if records and pg_hook_landing.cursor:
        #     csv_writer.writerow([col[0] for col in pg_hook_landing.cursor.description])
        csv_writer.writerows(records)
    print(f"Data from {table_name} exported to {output_filepath}")


with DAG(
    dag_id='export_csv',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None, # Or define your desired schedule
    catchup=False,
    tags=['csv', 'data_ingestion'],
) as dag:
    export_task = PythonOperator(
        task_id="export_data_to_csv",
        python_callable=export_postgres_to_csv, 
        op_kwargs={
                'table_name': 'public.test_airflow',  # Replace with your table name
                'output_filepath': '/home/thnghi/airflow6/output/test.csv', # Adjust path as needed
                'postgres_conn_id': 'postgres_landdb' # Ensure this connection exists in Airflow UI
            }
    )
