from .settings import Settings
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
class MasterRunWorker():
     
    def __init__(self, file_path, import_by): 
        self.conn_landdb = Settings.CONN_LANDDB
        self.file_path = file_path
        self.import_master_key = None 
        self.import_by = import_by
    
    def create_import_master_run(self):
        print(f'Start import run: {datetime.now().strftime("%Y-%m-%d")}')  
        pg_hook_landing = PostgresHook(postgres_conn_id=self.conn_landdb)
        self.import_master_key = pg_hook_landing.get_records(f"SELECT v_IMPORT_MASTER_KEY FROM  fn_import_create_import_master_run('{self.file_path}','{self.import_by}' );")[0][0] 

 

 