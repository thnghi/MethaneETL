from .settings import Settings
from airflow.hooks.postgres_hook import PostgresHook
import csv
import tempfile
import os  
from worker.common import *

class BaseWorker():
     
    def __init__(self, pg_hook_landing,pg_hook_methane, file_path, import_operation_key): 
        self.file_path = file_path
        self.import_operation_key = import_operation_key 
        self.pg_hook_landing = pg_hook_landing
        self.pg_hook_methane = pg_hook_methane
        self.table_name = "" 
        self.message = "" 
        self.status = "" 
        self.temp_file_path = ""

    def create_temp_file_with_import_operation_key(self): 
        with tempfile.NamedTemporaryFile(mode='w+', newline='', delete=False) as temp_file:
            writer = csv.writer(temp_file) 
            with open(self.file_path, 'r', newline='') as infile:
                reader = csv.reader(infile)
                headers = next(reader)
                writer.writerow(headers + ['IMPORT_OPERATION_KEY'])

                for row in reader:
                    writer.writerow(row + [self.import_operation_key])

            self.temp_file_path = temp_file.name  

    def clean_up_landing_data(self):
        self.pg_hook_landing.run(f"delete from {self.table_name} where import_operation_key = ({self.import_operation_key});")

    def start_import_operation(self):
        self.pg_hook_landing.run(f"""
        update public.IMPORT_OPERATION_TABLE set 
            STATUS = 'RUNNING', 
            START_TIME = current_timestamp, 
            MESSAGE = null 
        where import_operation_key = {self.import_operation_key};
        """)
    
    def complete_import_operation(self):
        self.message = self.message.replace("'","''")[:300]

        self.pg_hook_landing.run(f"""
        update public.IMPORT_OPERATION_TABLE set 
            STATUS = '{self.status}', 
            END_TIME = current_timestamp, 
            MESSAGE = '{self.message}' 
        where import_operation_key = {self.import_operation_key};
        """)
        processed_file_path = self.file_path.replace(f"{Settings.INPROGRESS_PATH}/",f"{Settings.PROCESSED_PATH}/") 
        move_file(self.file_path, processed_file_path) 
 