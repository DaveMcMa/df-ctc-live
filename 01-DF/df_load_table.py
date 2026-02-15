# THIS ONE LOADS DATA.IBT FILES FROM MINIO BUCKET INTO HBASE TABLE
# WITH TRACKING TO AVOID REPROCESSING FILES

import os
import sys
import requests
import logging
from logging.handlers import RotatingFileHandler 
import json
import urllib3
import base64
import irsdk
import pandas as pd
import csv
import time
import yaml
import uuid
from datetime import datetime
from minio import Minio
from minio.error import S3Error
from HBaseRest import HBaseRest, HBaseRestTable

# disable unsigned HTTPS certificate warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

print("====== MOTORSPORT DATA LOADING SCRIPT (MinIO Edition) ======")
print("Script execution started at:", time.strftime("%Y-%m-%d %H:%M:%S"))

#### CONFIGURATION SECTION ####
# Application Configuration
app_name = 'motorsport_demo'
log_level = 'INFO'
log_file = './logs/motorsport_demo.log'

# Data Fabric Configuration
datafabric_cluster_ip = '10.1.84.210'
datafabric_cluster = 'ctc-core'
datafabric_port = '8443'

# Authentication
user = 'mapr'
password = 'mapr123'

# Volume and Table Configuration
datafabric_volume_mount_path = '/ctc'
table_name = 'ctc-table'

# HBase REST Configuration
hbase_rest_node_ip = '10.1.84.212'
hbase_rest_node = 'ezdf-core3.ezmeral.demo.local'
hbase_rest_port = '8080'

# MinIO Configuration
minio_endpoint = "ezdf-core1.ezmeral.demo.local:9000"
minio_bucket = "f1ctc"
minio_access_key = "ARCKIQLRTTIWM17FJ9EKSQ4SZWIJADNBPKRCTD9FMJ6FHYEMA7WV0BJJCUC5YBD5XPWKCG95"
minio_secret_key = "0X9NRDI6OE80X4LRQJDE05B221L3UFX7R5OXSO6D7KWQFTYSD1BHBY2E1UFHMW6VNJP72JSRRYL5T07CTTZ25"

# Temp directory for downloaded files
temp_directory = './temp'

# HBase Column Families
telemetry_column_family = 'telemetry'
weekenddata_column_family = 'metadata'
file_tracking_column_family = 'file_metadata'

# Columns to Extract from IBT File
columns_of_interest = 'SessionTime,SessionTick,SessionUniqueID,Lap,LapCurrentLapTime,LapLastLapTime,LapBestLapTime,Lat,Lon,Yaw,YawNorth,Pitch,Roll,Speed,VelocityX,VelocityY,VelocityZ,Throttle,Brake,Clutch,Gear,RPM,SteeringWheelAngle,LFshockDefl,RFshockDefl,LRshockDefl,RRshockDefl,LFshockVel,RFshockVel,LRshockVel,RRshockVel,LFtempCL,LFtempCM,LFtempCR,RFtempCL,RFtempCM,RFtempCR,LRtempCL,LRtempCM,LRtempCR,RRtempCL,RRtempCM,RRtempCR,LFwearL,LFwearM,LFwearR,RFwearL,RFwearM,RFwearR,LRwearL,LRwearM,LRwearR,RRwearL,RRwearM,RRwearR'

# Metadata Fields to Extract
metadata_field_list = ['DriverInfo_Username','WeekendInfo_TrackID', 'WeekendInfo_TrackDisplayName','WeekendInfo_TrackSurfaceTemp','WeekendInfo_WeekendOptions_TimeOfDay','WeekendInfo_WeekendOptions_Date','DriverInfo_Drivers_CarNumber','DriverInfo_Drivers_CarScreenName','CarSetup_TiresAero_TireType','Chassis_Front_ArbBlades','Chassis_LeftFront_CornerWeight','Chassis_LeftFront_RideHeight','Chassis_RightFront_CornerWeight','Chassis_RightFront_RideHeight','Chassis_LeftRear_CornerWeight','Chassis_LeftRear_RideHeight','Chassis_RightRear_CornerWeight','Chassis_RightRear_RideHeight']

#### END CONFIGURATION SECTION ####

print(f"[CONFIG] App name: {app_name}")
print(f"[CONFIG] Log level: {log_level}")
print(f"[CONFIG] Log file: {log_file}")
print(f"[CONFIG] Table name: {table_name}")
print(f"[CONFIG] MinIO endpoint: {minio_endpoint}")
print(f"[CONFIG] MinIO bucket: {minio_bucket}")
print(f"[CONFIG] Temp directory: {temp_directory}")
print(f"[CONFIG] Columns of interest: {columns_of_interest}")

# set parameters
datafabric_url = "https://" + datafabric_cluster_ip + ":" + datafabric_port
hbase_rest_url = "https://" + hbase_rest_node_ip + ":" + hbase_rest_port
table_path = (datafabric_volume_mount_path + "/" + table_name).replace("/","%2F")

print(f"[CONFIG] HBase REST URL: {hbase_rest_url}")
print(f"[CONFIG] Table path: {table_path}")


### functions

def create_logger(app_name, log_lev, log_file):
    print(f"[LOGGER] Creating logger with name: {app_name}, level: {log_lev}, file: {log_file}")
    logger = logging.getLogger(app_name)
    log_level = os.getenv('LOG_LEVEL')
    if not log_level: log_level = log_lev
    logger.setLevel(log_level)
    formatter = logging.Formatter("%(asctime)s [%(levelname)-5s]: %(message)s [%(funcName)s:%(lineno)d]")
    file_handler = RotatingFileHandler(log_file, maxBytes=(1048576*5), backupCount=7)
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = True
    print(f"[LOGGER] Logger created successfully")
    return(logger)


def get_processed_files(table_obj):
    """
    Query HBase to get list of already processed files
    Returns a dict: {filename: status}
    """
    print("[HBASE] Querying for previously processed files")
    processed_files = {}
    
    try:
        # Scan the table for all rows with "file:" prefix in file_metadata column family
        results = table_obj.scan_table_by_prefix("file:", file_tracking_column_family)
        
        # Convert to simple dict of {filename: status}
        for row_key, columns in results.items():
            # Extract filename from row key (format: "file:path/to/file.ibt")
            filename = row_key.replace("file:", "", 1)
            status = columns.get('status', 'unknown')
            processed_files[filename] = status
            print(f"[HBASE] Found processed file: {filename} (status: {status})")
        
        logger.info(f"Found {len(processed_files)} previously processed files")
        print(f"[HBASE] Found {len(processed_files)} previously processed files")
        
    except Exception as e:
        logger.warning(f"Could not retrieve processed files list: {str(e)}")
        print(f"[WARNING] Could not retrieve processed files: {str(e)}")
        print(f"[WARNING] Will process all files in bucket")
    
    return processed_files


def mark_file_processing(table_obj, file_name, status, row_count=0, error_msg=""):
    """
    Mark a file as being processed in HBase
    status: 'processing', 'complete', 'failed'
    """
    print(f"[HBASE] Marking file '{file_name}' with status: {status}")
    
    try:
        # Create row key from filename
        row_key = base64.b64encode(f"file:{file_name}".encode('utf-8')).decode('utf-8')
        
        # Build HBase row
        hbase_row = '{"Row":[{"key":"' + row_key + '","Cell":['
        
        # Add status
        hbase_row += '{"column":"' + base64.b64encode(f"{file_tracking_column_family}:status".encode('utf-8')).decode('utf-8') + '",'
        hbase_row += '"$":"' + base64.b64encode(status.encode('utf-8')).decode('utf-8') + '"},'
        
        # Add timestamp
        hbase_row += '{"column":"' + base64.b64encode(f"{file_tracking_column_family}:timestamp".encode('utf-8')).decode('utf-8') + '",'
        hbase_row += '"$":"' + base64.b64encode(datetime.now().isoformat().encode('utf-8')).decode('utf-8') + '"},'
        
        # Add row count
        hbase_row += '{"column":"' + base64.b64encode(f"{file_tracking_column_family}:row_count".encode('utf-8')).decode('utf-8') + '",'
        hbase_row += '"$":"' + base64.b64encode(str(row_count).encode('utf-8')).decode('utf-8') + '"},'
        
        # Add error message if failed
        if error_msg:
            hbase_row += '{"column":"' + base64.b64encode(f"{file_tracking_column_family}:error".encode('utf-8')).decode('utf-8') + '",'
            hbase_row += '"$":"' + base64.b64encode(error_msg.encode('utf-8')).decode('utf-8') + '"},'
        
        # Remove trailing comma and close
        hbase_row = hbase_row[:-1] + ']}]}'
        
        # Insert into HBase
        res = table_obj.insert(hbase_row)
        
        if res.status_code == 200:
            logger.info(f"Successfully marked file '{file_name}' as '{status}'")
            print(f"[HBASE] Successfully marked file '{file_name}' as '{status}'")
        else:
            logger.error(f"Failed to mark file '{file_name}': {res.status_code}")
            print(f"[ERROR] Failed to mark file '{file_name}': {res.status_code}")
            
    except Exception as e:
        logger.exception(f"Exception marking file '{file_name}': {str(e)}")
        print(f"[ERROR] Exception marking file '{file_name}': {str(e)}")


#### check if log path exists, create if not and create logger
print(f"[LOGGER] Checking if log directory exists: {os.path.dirname(log_file)}")
if not os.path.exists(os.path.dirname(log_file)): 
    print(f"[LOGGER] Creating log directory: {os.path.dirname(log_file)}")
    os.makedirs(os.path.dirname(log_file))

# create logger
try:
    if 'logger' not in globals():
        logger = create_logger(app_name, log_level, log_file)
        print("[LOGGER] Logger initialized successfully")
except Exception as e:
    print(f"[ERROR] Failed to create logger: {str(e)}")
    logging.error(str(e))

# create temp directory if it doesn't exist
if not os.path.exists(temp_directory):
    print(f"[TEMP] Creating temp directory: {temp_directory}")
    os.makedirs(temp_directory)


### Initialize MinIO client
print(f"[MINIO] Initializing MinIO client for endpoint: {minio_endpoint}")
try:
    minio_client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=True,
        http_client=urllib3.PoolManager(cert_reqs='CERT_NONE')
    )
    print("[MINIO] MinIO client initialized successfully")
    logger.info("MinIO client initialized")
except Exception as e:
    print(f"[ERROR] Failed to initialize MinIO client: {str(e)}")
    logger.exception(f"Failed to initialize MinIO client: {str(e)}")
    sys.exit(1)


### Get list of IBT files from MinIO bucket
print(f"[MINIO] Listing .ibt files in bucket: {minio_bucket}")
ibt_files_in_bucket = []
try:
    objects = minio_client.list_objects(minio_bucket, recursive=True)
    for obj in objects:
        if obj.object_name.endswith('.ibt'):
            ibt_files_in_bucket.append(obj.object_name)
            print(f"[MINIO] Found: {obj.object_name}")
    
    print(f"[MINIO] Total .ibt files found: {len(ibt_files_in_bucket)}")
    logger.info(f"Found {len(ibt_files_in_bucket)} .ibt files in bucket")
    
except S3Error as e:
    print(f"[ERROR] Failed to list objects in bucket: {str(e)}")
    logger.exception(f"Failed to list objects in bucket: {str(e)}")
    sys.exit(1)


### Initialize HBase table connection
print(f"[HBASE] Instantiating HBaseRestTable for table: {datafabric_volume_mount_path}/{table_name}")
table_iracing = HBaseRestTable(user, password, hbase_rest_node, hbase_rest_node_ip, hbase_rest_port, datafabric_volume_mount_path + "/" + table_name)
print("[HBASE] HBaseRestTable instantiated successfully")


### Get list of already processed files
processed_files = get_processed_files(table_iracing)
print(f"[HBASE] Previously processed files: {len(processed_files)}")


### Determine which files need to be processed
files_to_process = [f for f in ibt_files_in_bucket if f not in processed_files or processed_files.get(f) != 'complete']
print(f"[PROCESSING] Files to process: {len(files_to_process)}")

if len(files_to_process) == 0:
    print("[PROCESSING] No new files to process. Exiting.")
    logger.info("No new files to process")
    sys.exit(0)


### Process each file
for file_index, ibt_file_key in enumerate(files_to_process, 1):
    print(f"\n{'='*60}")
    print(f"[PROCESSING] File {file_index}/{len(files_to_process)}: {ibt_file_key}")
    print(f"{'='*60}")
    
    # Generate UUID for this file's data
    uuid_value = uuid.uuid4()
    print(f"[PROCESSING] Using UUID: {uuid_value}")
    
    # Local temp file path
    local_file_path = os.path.join(temp_directory, os.path.basename(ibt_file_key))
    
    # The actual file in MinIO has spaces, not + signs
    # list_objects returns + but we need to convert to spaces for download
    actual_minio_key = ibt_file_key.replace('+', ' ')
    
    try:
        # Mark as processing
        mark_file_processing(table_iracing, ibt_file_key, 'processing')
        
        # Download file from MinIO
        print(f"[MINIO] Downloading {ibt_file_key} (actual key: {actual_minio_key}) to {local_file_path}")
        logger.info(f"Downloading {ibt_file_key}")
        minio_client.fget_object(minio_bucket, actual_minio_key, local_file_path)
        print(f"[MINIO] Download complete: {local_file_path}")
        
        # Initialize irsdk
        if 'ir' not in globals():
            print("[IBT] Initializing irsdk.IBT() object")
            ir = irsdk.IBT()
            print("[IBT] irsdk.IBT() object initialized successfully")
        
        # Open IBT file
        print(f"[IBT] Opening file: {local_file_path}")
        logger.info(f"Opening file: {local_file_path}")
        ir.open(local_file_path)
        file_open_status = True
        print(f"[IBT] File opened successfully")
        
        # Extract telemetry data
        print(f"[IBT] Extracting telemetry data")
        df = pd.DataFrame()
        column_list = columns_of_interest.replace(" ", "").split(',')
        print(f"[IBT] Columns being extracted: {len(column_list)} columns")
        
        for param in column_list:
            data = ir.get_all(param)
            # Handle scalar values (convert to single-element list)
            if not hasattr(data, '__len__') or isinstance(data, (int, float)):
                data = [data]
            df = pd.concat([df, pd.DataFrame({param: data})], axis=1, ignore_index=False)
        
        print(f"[IBT] DataFrame created with {len(df)} rows and {len(df.columns)} columns")
        logger.info(f"Extracted {len(df)} rows of telemetry data")
        
        # Extract metadata
        print("[IBT] Extracting metadata from IBT file")
        ibt_meta = (ir._shared_mem[ir._header.session_info_offset:ir._header.session_info_offset+ir._header.session_info_len]).decode('unicode-escape')
        print(f"[IBT] Metadata extracted")
        
        # Close IBT file
        print(f"[IBT] Closing file: {local_file_path}")
        ir.close()
        file_open_status = False
        print(f"[IBT] File closed successfully")
        
        # Parse metadata
        print("[METADATA] Parsing metadata YAML")
        metadata = yaml.safe_load(ibt_meta)
        
        # Extract metadata fields with error handling
        print("[METADATA] Extracting metadata fields")
        try:
            DriverInfo_Username = metadata['DriverInfo']['Drivers'][0]['UserName']
        except:
            DriverInfo_Username = 'unknown'
        
        try:
            WeekendInfo_TrackID = metadata['WeekendInfo']['TrackID']
        except:
            WeekendInfo_TrackID = 'unknown'
        
        try:
            WeekendInfo_TrackDisplayName = metadata['WeekendInfo']['TrackDisplayName']
        except:
            WeekendInfo_TrackDisplayName = 'unknown'
        
        try:
            WeekendInfo_TrackSurfaceTemp = metadata['WeekendInfo']['TrackSurfaceTemp']
        except:
            WeekendInfo_TrackSurfaceTemp = 'unknown'
        
        try:
            WeekendInfo_WeekendOptions_TimeOfDay = metadata['WeekendInfo']['WeekendOptions']['TimeOfDay']
        except:
            WeekendInfo_WeekendOptions_TimeOfDay = 'unknown'
        
        try:
            WeekendInfo_WeekendOptions_Date = metadata['WeekendInfo']['WeekendOptions']['Date']
        except:
            WeekendInfo_WeekendOptions_Date = 'unknown'
        
        try:
            DriverInfo_Drivers_CarNumber = metadata['DriverInfo']['Drivers'][0]['CarNumber']
        except:
            DriverInfo_Drivers_CarNumber = 'unknown'
        
        try:
            DriverInfo_Drivers_CarScreenName = metadata['DriverInfo']['Drivers'][0]['CarScreenName']
        except:
            DriverInfo_Drivers_CarScreenName = 'unknown'
        
        try:
            CarSetup_TiresAero_TireType = metadata['CarSetup']['TiresAero']['TireType']
        except:
            CarSetup_TiresAero_TireType = 'unknown'
        
        try:
            Chassis_Front_ArbBlades = metadata['CarSetup']['Chassis']['Front']['ArbBlades']
        except:
            Chassis_Front_ArbBlades = 'unknown'
        
        try:
            Chassis_LeftFront_CornerWeight = metadata['CarSetup']['Chassis']['LeftFront']['CornerWeight']
        except:
            Chassis_LeftFront_CornerWeight = 'unknown'
        
        try:
            Chassis_LeftFront_RideHeight = metadata['CarSetup']['Chassis']['LeftFront']['RideHeight']
        except:
            Chassis_LeftFront_RideHeight = 'unknown'
        
        try:
            Chassis_RightFront_CornerWeight = metadata['CarSetup']['Chassis']['RightFront']['CornerWeight']
        except:
            Chassis_RightFront_CornerWeight = 'unknown'
        
        try:
            Chassis_RightFront_RideHeight = metadata['CarSetup']['Chassis']['RightFront']['RideHeight']
        except:
            Chassis_RightFront_RideHeight = 'unknown'
        
        try:
            Chassis_LeftRear_CornerWeight = metadata['CarSetup']['Chassis']['LeftRear']['CornerWeight']
        except:
            Chassis_LeftRear_CornerWeight = 'unknown'
        
        try:
            Chassis_LeftRear_RideHeight = metadata['CarSetup']['Chassis']['LeftRear']['RideHeight']
        except:
            Chassis_LeftRear_RideHeight = 'unknown'
        
        try:
            Chassis_RightRear_CornerWeight = metadata['CarSetup']['Chassis']['RightRear']['CornerWeight']
        except:
            Chassis_RightRear_CornerWeight = 'unknown'
        
        try:
            Chassis_RightRear_RideHeight = metadata['CarSetup']['Chassis']['RightRear']['RideHeight']
        except:
            Chassis_RightRear_RideHeight = 'unknown'
        
        print(f"[METADATA] Driver: {DriverInfo_Username}, Track: {WeekendInfo_TrackDisplayName}, Car: {DriverInfo_Drivers_CarScreenName}")
        
        # Create HBase metadata row
        index = 0
        print("[HBASE] Creating HBase row for metadata")
        hbase_meta_row = '{"Row":[' + '{"key":"' + base64.b64encode((str(uuid_value) + ":" + str(index)).encode('utf-8')).decode('utf-8') + '","Cell":['
        
        # add column with uuid
        hbase_meta_row = hbase_meta_row + '{"column":"' + base64.b64encode((weekenddata_column_family + ":" + "uuid").encode('utf-8')).decode('utf-8') + '","$":"' + base64.b64encode((str(uuid_value)).encode('utf-8')).decode('utf-8') + '"},'
        
        # add source file name
        hbase_meta_row = hbase_meta_row + '{"column":"' + base64.b64encode((weekenddata_column_family + ":" + "source_file").encode('utf-8')).decode('utf-8') + '","$":"' + base64.b64encode((str(ibt_file_key)).encode('utf-8')).decode('utf-8') + '"},'
        
        # loop over fields and create column syntax
        g = globals()
        for col_name in metadata_field_list:
            hbase_meta_row = hbase_meta_row + '{"column":"' + base64.b64encode((weekenddata_column_family + ":" + str(col_name)).encode('utf-8')).decode('utf-8') + '","$":"' + base64.b64encode((str(g[col_name])).encode('utf-8')).decode('utf-8') + '"},'
        
        hbase_meta_row = hbase_meta_row[:-1] + ']}' + ']}'
        print(f"[HBASE] Metadata row created")
        
        # Create HBase telemetry rows
        print("[HBASE] Creating payload for HBase REST call")
        logger.info("Creating payload for HBase REST call")
        
        # Filter DataFrame to include only the specified columns
        df = df[columns_of_interest.replace(" ", "").split(',')]
        
        hbase_rows = []
        # loop over df rows and create list of hbase rows
        print(f"[HBASE] Processing {len(df)} dataframe rows for HBase")
        row_count = 0
        print_interval = max(1, min(1000, len(df) // 10))
        
        for index, row in df.iterrows():
            row_count += 1
            if row_count % print_interval == 0 or row_count == 1 or row_count == len(df):
                print(f"[HBASE] Processing row {row_count} of {len(df)} ({row_count/len(df)*100:.1f}%)")
            
            hbase_row = '{"key":"' + base64.b64encode((str(uuid_value) + ":" + str(index)).encode('utf-8')).decode('utf-8') + '","Cell":['
            
            # add column with uuid
            hbase_row = hbase_row + '{"column":"' + base64.b64encode((telemetry_column_family + ":" + "uuid").encode('utf-8')).decode('utf-8') + '","$":"' + base64.b64encode((str(uuid_value)).encode('utf-8')).decode('utf-8') + '"},'
            
            # add column TrackID
            hbase_row = hbase_row + '{"column":"' + base64.b64encode((telemetry_column_family + ":" + "TrackID").encode('utf-8')).decode('utf-8') + '","$":"' + base64.b64encode((str(WeekendInfo_TrackID)).encode('utf-8')).decode('utf-8') + '"},'
            
            # add source file
            hbase_row = hbase_row + '{"column":"' + base64.b64encode((telemetry_column_family + ":" + "source_file").encode('utf-8')).decode('utf-8') + '","$":"' + base64.b64encode((str(ibt_file_key)).encode('utf-8')).decode('utf-8') + '"},'
            
            for col_name, value in row.items():
                hbase_row = hbase_row + '{"column":"' + base64.b64encode((telemetry_column_family + ":" + str(col_name)).encode('utf-8')).decode('utf-8') + '","$":"' + base64.b64encode((str(value)).encode('utf-8')).decode('utf-8') + '"},'
            
            hbase_row = hbase_row[:-1] + ']}'
            hbase_rows.append(hbase_row)
        
        # create string for REST payload
        print("[HBASE] Finalizing REST payload")
        hrows = '{"Row":' + str(hbase_rows).replace("'{","{").replace("}'","}") + '}'
        print(f"[HBASE] Payload ready, size: {len(hrows) / (1024*1024):.2f} MB")
        logger.info("Payload ready for HBase REST call")
        
        # Write metadata to HBase
        print("[HBASE] Starting metadata insertion")
        st = time.time()
        try:
            logger.info(f"Starting metadata load for file: {ibt_file_key}")
            res = table_iracing.insert(hbase_meta_row)
            print(f"[HBASE] Metadata insertion API call completed with status code: {res.status_code}")
        except Exception as e:
            print(f"[ERROR] Failed to load metadata: {str(e)}")
            logger.exception(f"Failed to load metadata: {str(e)}")
            mark_file_processing(table_iracing, ibt_file_key, 'failed', error_msg=str(e))
            # Clean up temp file
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
            continue
        
        et = time.time()
        if res.status_code == 200:
            print(f"[HBASE] Successfully loaded metadata. Execution time: {et - st:.2f} seconds")
            logger.info(f"Successfully loaded metadata. Execution time: {et - st:.2f} seconds")
        else:
            print(f"[ERROR] Failed to load metadata. Error code: {res.status_code}")
            logger.error(f"Failed to load metadata. Error code: {res.status_code}")
            mark_file_processing(table_iracing, ibt_file_key, 'failed', error_msg=f"HTTP {res.status_code}")
            # Clean up temp file
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
            continue
        
        # Write telemetry data to HBase
        print("[HBASE] Starting telemetry data insertion")
        st = time.time()
        try:
            logger.info(f"Starting telemetry data load for file: {ibt_file_key}")
            res = table_iracing.insert(hrows)
            print(f"[HBASE] Telemetry data insertion API call completed with status code: {res.status_code}")
        except Exception as e:
            print(f"[ERROR] Failed to load telemetry data: {str(e)}")
            logger.exception(f"Failed to load telemetry data: {str(e)}")
            mark_file_processing(table_iracing, ibt_file_key, 'failed', error_msg=str(e))
            # Clean up temp file
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
            continue
        
        et = time.time()
        if res.status_code == 200:
            print(f"[HBASE] Successfully loaded telemetry data. Execution time: {et - st:.2f} seconds")
            logger.info(f"Successfully loaded telemetry data. Execution time: {et - st:.2f} seconds")
            
            # Mark file as complete
            mark_file_processing(table_iracing, ibt_file_key, 'complete', row_count=len(df))
            print(f"[SUCCESS] File {ibt_file_key} processed successfully!")
            
        else:
            print(f"[ERROR] Failed to load telemetry data. Error code: {res.status_code}")
            logger.error(f"Failed to load telemetry data. Error code: {res.status_code}")
            mark_file_processing(table_iracing, ibt_file_key, 'failed', error_msg=f"HTTP {res.status_code}")
        
        # Clean up temp file
        if os.path.exists(local_file_path):
            print(f"[CLEANUP] Removing temp file: {local_file_path}")
            os.remove(local_file_path)
            
    except Exception as e:
        print(f"[ERROR] Exception processing file {ibt_file_key}: {str(e)}")
        logger.exception(f"Exception processing file {ibt_file_key}: {str(e)}")
        mark_file_processing(table_iracing, ibt_file_key, 'failed', error_msg=str(e))
        
        # Clean up temp file if it exists
        if os.path.exists(local_file_path):
            print(f"[CLEANUP] Removing temp file: {local_file_path}")
            os.remove(local_file_path)
        
        continue

print("\n" + "="*60)
print("====== MOTORSPORT DATA LOADING COMPLETE ======")
print(f"Processed {len(files_to_process)} files")
print("Script execution completed at:", time.strftime("%Y-%m-%d %H:%M:%S"))
print("="*60)