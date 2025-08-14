    
import csv
import json
from jsonschema import validate, ValidationError
from datetime import datetime
import os
import shutil
# Convert CSV to JSON and validate
def validate_csv(csv_file_path, schema):
    print("validate_csv")
    with open(csv_file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for i, row in enumerate(reader, start=1):
            print(row)
            # Convert values to appropriate types
            # row['age'] = int(row['age'])  # Example: cast age to integer
            try:
                validate(instance=row, schema=schema)
                print(f"Row {i} is valid.")
            except ValidationError as e:
                print(f"Row {i} is invalid: {e.message}")

            if i ==100: break
def replace_last(s, old_char, new_string):
    parts = s.rsplit(old_char, 1)
    return new_string.join(parts)

def copy_file(source_file, dest_file):
    date_folder =  datetime.now().strftime("%Y/%m/%d")
    file_name = dest_file.split("/")[-1]
    directory_path = dest_file.replace(file_name,"")
    os.makedirs(directory_path, exist_ok=True)
    shutil.copy(source_file, dest_file)

def dummy_test_file(import_type):
    date_folder =  datetime.now().strftime("%Y/%m/%d")
    file_name = "pedgiree_10k"
    source_file = f"/home/thnghi/airflow6/ingestion/sample_data/{file_name}.csv"

    for i in range(1, 21):
        for j in range(1,4):
            dest_file = f"/home/thnghi/airflow6/ingestion/unprocess/{date_folder}/{import_type}/user{i}/pedigree/{file_name}_{j}.csv" 
            copy_file(source_file,dest_file) 

dummy_test_file("UserUpload")
dummy_test_file("SystemUpload")
# Define your JSON Schema
# schema = {
#     "type": "object",
#     "properties": {
#         "ANIMAL":   {"type": "string","format": "date"},
#         "NAME":    {"type": "string","minLength": 1},
#         "DOB":    {"type": "string","format": "date"},
#         "NM$_PTA": {
#                     "anyOf": [
#                         { "type": "integer" },
#                         { "type": "string", "pattern": "^[0-9]+$" }
#                     ]
#                     }
#     },
#     "required": ["ANIMAL","NAME", "DOB"]
# }
  
# csv_file_path = "/home/thnghi/airflow6/ingestion/inprogress/2025/08/05/user1/animal/input_test_animal_6MB.csv"""
# validate_csv(csv_file_path, schema)

# file_path = "/home/thnghi/airflow6/ingestion/inprogress/2025/08/11/user1/pedigree/pedgiree_10k.csv"
# unix_timestamp = datetime.now().strftime("_%Y%m%d%H%M%S.")
# new_file_path = replace_last(file_path,'.',unix_timestamp)
# file_name = file_path.split("/")[-1]
# file_extension = file_name.split(".")[-1]

# new_file_name = unix_timestamp+'.'+file_extension

# print(f"file_name: {file_name}")
# print(f"unix_timestamp: {unix_timestamp}")
# print(new_file_path)
