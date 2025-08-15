from .settings import Settings
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
class UserRunWorker():
     
    def __init__(self,pg_hook_landing, file_path = None, src_user = None, import_master_key = None,import_by=None): 
        self.conn_landdb = Settings.CONN_LANDDB
        self.file_path = file_path
        self.src_user = src_user
        self.import_master_key = import_master_key
        self.import_key = None
        self.pg_hook_landing = pg_hook_landing
        self.run_batch = Settings.RUN_BATCH
        self.import_by = import_by
    def create_import_run(self):  
        self.import_key = self.pg_hook_landing.get_records(f"SELECT * FROM  fn_import_create_import_run('{self.file_path}','{self.src_user}','{self.import_by}',{self.import_master_key} );")[0][0] 
 
    def create_import_operation(self,file_name,file_type):  
        self.pg_hook_landing.get_records(f"SELECT * FROM  fn_import_create_import_operation({self.import_key},'{file_name}','{file_type}');") 

    def get_list_import_operation_to_be_run(self):  
        return self.pg_hook_landing.get_records(f"""
        
       SELECT 
              t.IMPORT_OPERATION_KEY,
	   		  t.FILE_TYPE,
			  t.FILE_NAME,
              t.IMPORT_KEY
	   FROM public.IMPORT_OPERATION_TABLE t
	   WHERE STATUS = 'CREATED'
       AND IMPORT_KEY IN
	   ( 
			SELECT IMPORT_KEY
		    FROM public.IMPORT_RUN_TABLE
		    WHERE STATUS = 'CREATED'
            ORDER BY IMPORT_KEY
		    LIMIT {self.run_batch}

	   )
       ;
         """)
 
    def start_import_run(self, import_key_list):  
        self.pg_hook_landing.run(f"""
        update public.IMPORT_RUN_TABLE set 
            STATUS = 'RUNNING', 
            START_TIME = current_timestamp, 
            MESSAGE = null 
        where import_key in ({import_key_list});
        """)
    
    def finish_import_run(self, import_key_list):  
        self.pg_hook_landing.run(f"call public.usp_update_import_runs('{import_key_list}');") 
 
 
 