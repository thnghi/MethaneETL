    
import csv
import json
from jsonschema import validate, ValidationError
from datetime import datetime
import os
import shutil
import requests
import time
# Convert CSV to JSON and validate
def validate_csv(csv_file_path, schema):
    print("validate_csv")
    with open(csv_file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for i, row in enumerate(reader, start=1):
            print(row)
            # Convert values to appropriate types
            # row['age'] = int(row['age'])  # Example: cast age to integer
            try:
                validate(instance=row, schema=schema)
                print(f"Row {i} is valid.")
            except ValidationError as e:
                print(f"Row {i} is invalid: {e.message}")

            if i ==100: break
def replace_last(s, old_char, new_string):
    parts = s.rsplit(old_char, 1)
    return new_string.join(parts)

def copy_file(source_file, dest_file):
    date_folder =  datetime.now().strftime("%Y/%m/%d")
    file_name = dest_file.split("/")[-1]
    directory_path = dest_file.replace(file_name,"")
    os.makedirs(directory_path, exist_ok=True)
    shutil.copy(source_file, dest_file)

def dummy_test_file(import_type):
    date_folder =  datetime.now().strftime("%Y/%m/%d")
    file_name = "pedgiree_10k"
    source_file = f"/home/thnghi/airflow6/ingestion/sample_data/{file_name}.csv"

    for i in range(1, 21):
        for j in range(1,4):
            dest_file = f"/home/thnghi/airflow6/ingestion/unprocess/{date_folder}/{import_type}/user{i}/pedigree/{file_name}_{j}.csv" 
            copy_file(source_file,dest_file) 

 
def wait_for_dag_run_complete(
    dag_id: str,
    run_id: str,
    airflow_base_url: str = "http://localhost:8080",
    poll_interval: int = 30,
    timeout: int = 1800,
    auth: tuple = ("admin", "12345678")  # username, password
):
    """
    Waits for a DAG run to complete by polling the Airflow REST API.

    :param dag_id: ID of the DAG to monitor
    :param run_id: Run ID of the DAG run
    :param airflow_base_url: Base URL of the Airflow webserver
    :param poll_interval: Seconds between each status check
    :param timeout: Max time to wait in seconds
    :param auth: Tuple of (username, password) for basic auth
    """
    endpoint = f"{airflow_base_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
    start_time = time.time()

    while True:
        response = requests.get(endpoint, auth=auth)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch DAG run status: {response.text}")

        status = response.json().get("state")
        print(f"[{dag_id} | {run_id}] Status: {status}")

        if status in ["success", "failed"]:
            return status

        if time.time() - start_time > timeout:
            raise TimeoutError(f"DAG run {run_id} did not complete within {timeout} seconds.")

        time.sleep(poll_interval)
  
dag_id = "F2L_consumer_child_dag"
unique_run_id = "F2L_consumer_child_dag__3d02c312-e4c8-43d4-9737-7d0a09634815"


wait_for_dag_run_complete(dag_id,unique_run_id)
# response = requests.get(endpoint, auth=auth)
# print(response)

# ls = [(53, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user1/pedigree/pedgiree_10k_1_20250812154435.csv', 53), (54, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user1/pedigree/pedgiree_10k_2_20250812154435.csv', 53), (55, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user1/pedigree/pedgiree_10k_3_20250812154435.csv', 53), (56, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user10/pedigree/pedgiree_10k_1_20250812154436.csv', 54), (57, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user10/pedigree/pedgiree_10k_2_20250812154436.csv', 54), (58, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user10/pedigree/pedgiree_10k_3_20250812154436.csv', 54), (59, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user11/pedigree/pedgiree_10k_1_20250812154436.csv', 55), (60, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user11/pedigree/pedgiree_10k_2_20250812154436.csv', 55), (61, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user11/pedigree/pedgiree_10k_3_20250812154437.csv', 55), (62, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user12/pedigree/pedgiree_10k_1_20250812154437.csv', 56), (63, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user12/pedigree/pedgiree_10k_2_20250812154437.csv', 56), (64, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user12/pedigree/pedgiree_10k_3_20250812154437.csv', 56), (65, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user13/pedigree/pedgiree_10k_1_20250812154437.csv', 57), (66, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user13/pedigree/pedgiree_10k_2_20250812154437.csv', 57), (67, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user13/pedigree/pedgiree_10k_3_20250812154437.csv', 57), (68, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user14/pedigree/pedgiree_10k_1_20250812154438.csv', 58), (69, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user14/pedigree/pedgiree_10k_2_20250812154438.csv', 58), (70, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user14/pedigree/pedgiree_10k_3_20250812154438.csv', 58), (71, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user15/pedigree/pedgiree_10k_1_20250812154438.csv', 59), (72, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user15/pedigree/pedgiree_10k_2_20250812154438.csv', 59), (73, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user15/pedigree/pedgiree_10k_3_20250812154439.csv', 59), (74, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user16/pedigree/pedgiree_10k_1_20250812154439.csv', 60), (75, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user16/pedigree/pedgiree_10k_2_20250812154439.csv', 60), (76, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user16/pedigree/pedgiree_10k_3_20250812154439.csv', 60), (77, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user17/pedigree/pedgiree_10k_1_20250812154439.csv', 61), (78, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user17/pedigree/pedgiree_10k_2_20250812154440.csv', 61), (79, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user17/pedigree/pedgiree_10k_3_20250812154440.csv', 61), (80, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user18/pedigree/pedgiree_10k_1_20250812154440.csv', 62), (81, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user18/pedigree/pedgiree_10k_2_20250812154440.csv', 62), (82, 'pedigree', '/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user18/pedigree/pedgiree_10k_3_20250812154440.csv', 62)]
# distinct_last =  ','.join(str(item) for item in {item[3]  for item in ls})    
# print(distinct_last)

# ls_pedigree = [{"import_operation_key": 56, "table_name": "public.LND_PEDIGREE_TABLE", "file_name": "/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user10/pedigree/pedgiree_10k_1_20250812154436.csv", "import_key": 54}, {"import_operation_key": 57, "table_name": "public.LND_PEDIGREE_TABLE", "file_name": "/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user10/pedigree/pedgiree_10k_2_20250812154436.csv", "import_key": 54}, {"import_operation_key": 58, "table_name": "public.LND_PEDIGREE_TABLE", "file_name": "/home/thnghi/airflow6/ingestion/inprogress/2025/08/12/UserUpload/user10/pedigree/pedgiree_10k_3_20250812154436.csv", "import_key": 54}]

# json_string = json.dumps(ls_pedigree)
# print(json_string)

# Define your JSON Schema
# schema = {
#     "type": "object",
#     "properties": {
#         "ANIMAL":   {"type": "string","format": "date"},
#         "NAME":    {"type": "string","minLength": 1},
#         "DOB":    {"type": "string","format": "date"},
#         "NM$_PTA": {
#                     "anyOf": [
#                         { "type": "integer" },
#                         { "type": "string", "pattern": "^[0-9]+$" }
#                     ]
#                     }
#     },
#     "required": ["ANIMAL","NAME", "DOB"]
# }
  
# csv_file_path = "/home/thnghi/airflow6/ingestion/inprogress/2025/08/05/user1/animal/input_test_animal_6MB.csv"""
# validate_csv(csv_file_path, schema)

# file_path = "/home/thnghi/airflow6/ingestion/inprogress/2025/08/11/user1/pedigree/pedgiree_10k.csv"
# unix_timestamp = datetime.now().strftime("_%Y%m%d%H%M%S.")
# new_file_path = replace_last(file_path,'.',unix_timestamp)
# file_name = file_path.split("/")[-1]
# file_extension = file_name.split(".")[-1]

# new_file_name = unix_timestamp+'.'+file_extension

# print(f"file_name: {file_name}")
# print(f"unix_timestamp: {unix_timestamp}")
# print(new_file_path)
