import os
import shutil
from datetime import datetime
import requests
import time
from worker.settings import *
# from settings import Authentication
def get_folder_to_process(directory_path): 
    subfolders = [f for f in os.listdir(directory_path) if os.path.isdir(os.path.join(directory_path, f))]
    return subfolders
 
def get_file_to_process(directory_path):  
    listfile = []
    for root, _, files in os.walk(directory_path):
        for file in files:
            if "Zone.Identifier" in file:
                continue
            file_name = os.path.join(root, file) 
            file_type = root.split("/")[-1] 
            listfile.append({"file_name": file_name,"file_type":file_type}) 
    return listfile

def move_file(src_file, dest_file):   
    dest_dir = dest_file.replace(dest_file.split("/")[-1],"") 
    os.makedirs(dest_dir, exist_ok=True)
    shutil.move(src_file, dest_file)

def replace_last(s, old_char, new_string):
    parts = s.rsplit(old_char, 1)
    return new_string.join(parts)

def generate_new_file_name(file_path):
    unix_timestamp = datetime.now().strftime("_%Y%m%d%H%M%S.") 
    return replace_last(file_path,'.',unix_timestamp) 

def try_delete_file(file_path):
    try:
        os.remove(file_path)
    except: pass 
 
def wait_for_dag_run_complete(
    dag_id: str,
    run_id: str,
    airflow_base_url: str = Authentication.airflow_base_url,
    poll_interval: int = 5,
    timeout: int = 1800,
    auth: tuple = (Authentication.user_name, Authentication.password)  # username, password
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
   