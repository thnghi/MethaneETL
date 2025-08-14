from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd # You'll likely need pandas for processing CSVs

def process_csv_file(**kwargs):
    # Define the path to your CSV file
    csv_file_path = "/home/thnghi/airflow6/source/test.csv" # Adjust this path as needed

    try:
        # Read the CSV file into a Pandas DataFrame
        df = pd.read_csv(csv_file_path)
        print("CSV file loaded successfully:")
        print(df.head()) # Print first few rows for verification

        # Perform further processing on the DataFrame (e.g., transformation, loading to a database)
        # ...

    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

with DAG(
    dag_id='csv_import_example',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None, # Or define your desired schedule
    catchup=False,
    tags=['csv', 'data_ingestion'],
) as dag:
    import_and_process_task = PythonOperator(
        task_id='import_and_process_csv',
        python_callable=process_csv_file,
        provide_context=True, # Allows passing Airflow context to the callable
    )