from .settings import Settings
from airflow.hooks.postgres_hook import PostgresHook 
import tempfile
import os
from worker.common import *
from worker.base_worker import BaseWorker
class PedigreeWorker(BaseWorker):
     
    def __init__(self, file_path, import_operation_key):
        super().__init__(file_path, import_operation_key)  
        self.table_name = Settings.PEDIGREE_TABLE   
        self.clean_up_landing_data();
        self.is_failed= False
         
    def process_file(self): 
        #Import in Landing
        
        self.import_to_landdb()

        if not self.is_failed:
            #Validate in Landing
            self.validate_in_landing() 
        if not self.is_failed:
            #Deliver to Methane DB
            self.deliver_to_methaneDB()
        
        if not self.is_failed:
            self.status = 'COMPLETED'
        self.complete_import_operation();

        

    def import_to_landdb(self):  
        try:
            self.start_import_operation();
            self.create_temp_file_with_import_operation_key();

            self.pg_hook_landing.copy_expert(
                f"COPY {self.table_name} (RECORD_TYPE,INTERNATIONAL_ID,TRANSACTION_TYPE,SIRE_ID,DAM_ID,BIRTH_DATE,NAME,SPECIES_CD,SOURCE_CD,IMPORT_OPERATION_KEY)  FROM STDIN WITH CSV HEADER",
            self.temp_file_path
            ) 
            self.status = 'C_F2L'
        except Exception as e: 
            self.status = 'F_F2L'
            self.message =f"Error F2L : {str(e)}" 
            self.is_failed= True
            print(self.message)
                
        finally: 
            try_delete_file(self.temp_file_path)
           

    def validate_in_landing(self):
        try:
            self.pg_hook_landing.run(f"call public.usp_validate_pedigree({self.import_operation_key});") 
            self.status = 'C_VAL'
        except Exception as e: 
            self.status = 'F_VAL'
            self.message =f"Error validate : {str(e)}" 
            self.is_failed= True
            print(self.message)  
         
    
    def deliver_to_methaneDB(self):
        try:
            self.pg_hook_methane.run(f"call public.usp_l2m_animal({self.import_operation_key});")
            self.pg_hook_methane.run(f"call public.usp_l2m_pedigree({self.import_operation_key});")
            self.status = 'C_VAL'
        except Exception as e: 
            self.status = 'F_VAL'
            self.message =f"Error validate : {str(e)}" 
            self.is_failed= True
            print(self.message) 
        


    # def staging_post_processing(self):
    #     print(f"staging_post_processing: {self.table_name}")

 