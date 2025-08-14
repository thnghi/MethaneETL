class Settings: 
    INGESTION_PATH = "/home/thnghi/airflow6/ingestion"
    UNPROCESS_PATH = f"{INGESTION_PATH}/unprocess"
    PROCESSED_PATH = f"{INGESTION_PATH}/processed"
    INPROGRESS_PATH = f"{INGESTION_PATH}/inprogress"
    FAILED_PATH = f"{INGESTION_PATH}/failed"
    CONN_LANDDB = "postgres_landdb"
    CONN_METHANEDB = "postgres_methanedb"  
    SRC_USER = "ALL"
    IS_INCLUDE_HISTORY_FILE = False
    RUN_BATCH = 5
    PEDIGREE_TABLE = "public.LND_PEDIGREE_TABLE"
    CALVING_TABLE = "public.LND_CALVING_TABLE"

class Authentication: 
    user_name = "admin"
    password = "12345678"
    airflow_base_url = "http://localhost:8080"