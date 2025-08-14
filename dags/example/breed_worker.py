
from .settings import Settings
from airflow.hooks.postgres_hook import PostgresHook 
import tempfile
import os
from worker.base_worker import BaseWorker
class BreedWorker(BaseWorker):
     
    def __init__(self, file_path, import_operation_key):
        super().__init__(file_path, import_operation_key)  
        self.table_name = Settings.BREED_TABLE    
     
    def import_to_landdb(self): 

        self.create_temp_file_with_import_operation_key();
        self.pg_hook_landing.copy_expert(
            f"COPY {self.table_name} (BREED_CODE,BREED_NAME,IMPORT_OPERATION_KEY)  FROM STDIN WITH CSV HEADER",
          self.temp_file_path
        ) 

        os.remove(self.temp_file_path)
         

    def import_validate(self):
        print(f"import_validate: {self.table_name}")
    
    def delivery_landdb_to_staging(self):
        print(f"delivery_landdb_to_staging: {self.table_name}")

    def staging_post_processing(self):
        print(f"staging_post_processing: {self.table_name}")