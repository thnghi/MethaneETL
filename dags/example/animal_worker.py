from .settings import Settings
from airflow.hooks.postgres_hook import PostgresHook 
import tempfile
import os
from worker.base_worker import BaseWorker
class AnimalWorker(BaseWorker):
     
    def __init__(self, file_path, import_operation_key):
        super().__init__(file_path, import_operation_key)  
        self.table_name = Settings.ANIMAL_TABLE   
        self.clean_up_landing_data();
         
     
    def import_to_landdb(self): 
        #Import in Landing
        self.create_temp_file_with_import_operation_key();
        self.pg_hook_landing.copy_expert(
            f"COPY {self.table_name} (ANIMAL,NAME,DOB,SIRE,NM_PTA,IMPORT_OPERATION_KEY)  FROM STDIN WITH CSV HEADER",
          self.temp_file_path
        ) 

        os.remove(self.temp_file_path)

        #Validate in Landing
        self.validate_in_landing() 
        #Deliver to Methane DB
        self.deliver_to_methaneDB()

    def validate_in_landing(self):
        self.pg_hook_landing.run(f"call public.usp_validate_test_animal({self.import_operation_key});")
    
    def deliver_to_methaneDB(self):
        self.pg_hook_methane.run(f"call public.usp_l2m_test_animal({self.import_operation_key});")



    def staging_post_processing(self):
        print(f"staging_post_processing: {self.table_name}")