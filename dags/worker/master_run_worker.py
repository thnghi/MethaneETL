from .settings import Settings 
class MasterRunWorker():
     
    def __init__(self, pg_hook_landing, file_path, import_by):  
        self.file_path = file_path
        self.import_master_key = None 
        self.import_by = import_by
        self.pg_hook_landing = pg_hook_landing
    
    def create_import_master_run(self): 
        self.import_master_key = self.pg_hook_landing.get_records(f"SELECT v_IMPORT_MASTER_KEY FROM  fn_import_create_import_master_run('{self.file_path}','{self.import_by}' );")[0][0] 
 
 

 