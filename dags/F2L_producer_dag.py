from __future__ import annotations
import pendulum 
from airflow.models.dag import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import task
from datetime import datetime


from worker.settings import Settings
from worker.common import *
from worker.master_run_worker import MasterRunWorker
from worker.user_run_worker import UserRunWorker

with DAG(
    dag_id="F2L_producer_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    #schedule_interval = '*/2 * * * *',
    catchup=False,
    tags=["landing"],
) as dag:

    @task
    def task_scan_file_and_create_run(upload_type):
        try:
            date_folder =  datetime.now().strftime("%Y/%m/%d") if Settings.IS_INCLUDE_HISTORY_FILE == False else ''
            source_user =  Settings.SRC_USER if Settings.SRC_USER != 'ALL' else '' 
            
            directory_path = f"{Settings.UNPROCESS_PATH}/{date_folder}/{upload_type}/{source_user}" 

            ls_src_user = get_folder_to_process(directory_path)
            list_file_user = []
            for src_user in ls_src_user:
                directory_path_user = f"{directory_path}{src_user}" 
                list_file = get_file_to_process(directory_path_user) 
                if len(list_file)>0:
                    list_file_user.append({"directory_path_user":directory_path_user,"src_user":src_user, "list_file":get_file_to_process(directory_path_user) })
                
            if len(list_file_user)>0: 
                master_worker = MasterRunWorker(directory_path,upload_type)  
                master_worker.create_import_master_run() 
                for file_user in list_file_user:
                    directory_path_user = file_user["directory_path_user"]
                    src_user = file_user["src_user"]
                    list_file = file_user["list_file"] 
                    run_worker = UserRunWorker(directory_path_user,src_user,master_worker.import_master_key,upload_type)  
                    run_worker.create_import_run()   
                    for f in list_file: 
                        f_unprogress = f["file_name"]  
                        f_inprogess = f_unprogress.replace(f"{Settings.UNPROCESS_PATH}/",f"{Settings.INPROGRESS_PATH}/") 
                        f_inprogess = generate_new_file_name(f_inprogess)
                        run_worker.create_import_operation(f_inprogess,f["file_type"])   
                        move_file(f_unprogress, f_inprogess) 
            else:
                print(f"No files found on {directory_path}")

        except Exception as e:
                if 'No such file or directory' in str(e):
                    print(f"No files found! detail: {str(e)}")
            

    task_scan_file_and_create_run("UserUpload") 
    task_scan_file_and_create_run("SystemUpload") 