import os
from minio import Minio
from minio.error import S3Error
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ENDPOINT = "ezdf-core1.ezmeral.demo.local:9000"
BUCKET_NAME = "f1ctc"
SCAN_DIRECTORY = r"C:\Users\ctc\Documents\iRacing\telemetry"
ACCESS_KEY = ""  
SECRET_KEY = ""  

client = Minio(
    ENDPOINT,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=True,
    http_client=urllib3.PoolManager(cert_reqs='CERT_NONE')
)

# Get existing files
existing_files = set()
try:
    objects = client.list_objects(BUCKET_NAME, recursive=True)
    existing_files = {obj.object_name for obj in objects}
except S3Error as e:
    print(f"Error listing: {e}")

# Upload files
for root, _, files in os.walk(SCAN_DIRECTORY):
    for file in files:
        local_path = os.path.join(root, file)
        s3_key = os.path.relpath(local_path, SCAN_DIRECTORY).replace('\\', '/')
        
        if s3_key in existing_files:
            print(f"Skip: {s3_key}")
            continue
        
        try:
            client.fput_object(BUCKET_NAME, s3_key, local_path)
            print(f"Uploaded: {s3_key}")
        except S3Error as e:
            print(f"Error: {s3_key} - {e}")