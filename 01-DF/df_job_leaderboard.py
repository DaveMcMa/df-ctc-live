#!/usr/bin/env python3
"""
Leaderboard Computation Job

This script scans the main telemetry table (/ctc/ctc-table), identifies the 
top 10 fastest laps for each track, and stores them in the leaderboard table 
(/ctc/leaderboard-table).

When new faster laps are found, it overwrites the previous leaderboard for that track.

Usage:
    python leaderboard_compute.py
"""

import sys
import base64
import json
import requests
import urllib3
from datetime import datetime
from collections import defaultdict

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================================
# CONFIGURATION
# ============================================================================
# HBase Connection
HBASE_USER = 'mapr'
HBASE_PASSWORD = 'mapr123'
HBASE_REST_NODE_IP = '10.1.84.212'
HBASE_REST_NODE = 'ezdf-core3.ezmeral.demo.local'
HBASE_REST_PORT = '8080'

# Tables
MAIN_TABLE_PATH = '/ctc/ctc-table'
LEADERBOARD_TABLE_PATH = '/ctc/leaderboard-table'

# Scanning
SCANNER_BATCH_SIZE = 100000

# Leaderboard Configuration
TOP_N_LAPS = 10  # Number of top laps to keep per track

# Column Selection for Scanning
MINIMAL_COLUMNS = [
    'telemetry:Lap',
    'telemetry:LapLastLapTime',
    'telemetry:LapCurrentLapTime',
    'telemetry:uuid',
    'telemetry:TrackID',
    'telemetry:SessionTick',
    'telemetry:source_file',
    'metadata:DriverInfo_Username',
    'metadata:DriverInfo_Drivers_CarScreenName',
    'metadata:DriverInfo_Drivers_CarNumber',
    'metadata:WeekendInfo_TrackDisplayName',
    'metadata:WeekendInfo_WeekendOptions_Date'
]

# Data Validation
MIN_LAP_TIME = 30  # Minimum realistic lap time in seconds
MAX_LAP_TIME = 600  # Maximum realistic lap time in seconds
MIN_DATA_POINTS = 100  # Minimum telemetry points for a valid lap

# ============================================================================
# HBase REST Client
# ============================================================================

class HBaseRestClient:
    def __init__(self, user, password, rest_node, rest_node_ip, rest_port):
        self.user = user
        self.password = password
        self.url = f"https://{rest_node}:{rest_port}"
    
    def create_scanner(self, table_path, scanner_filter):
        """Create a scanner for the table"""
        url = f"{self.url}/{table_path}/scanner"
        headers = {'Accept': 'application/xml', 'Content-Type': 'text/xml'}
        
        response = requests.put(
            url, 
            data=scanner_filter, 
            auth=(self.user, self.password), 
            headers=headers, 
            verify=False
        )
        
        if response.status_code == 201:
            return response.headers.get("Location")
        else:
            raise Exception(f"Failed to create scanner: {response.status_code} - {response.text}")
    
    def read_scanner(self, scanner_url):
        """Read batch from scanner"""
        response = requests.get(
            scanner_url, 
            auth=(self.user, self.password), 
            headers={'Accept': 'application/json'}, 
            verify=False
        )
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 204:
            return None
        else:
            raise Exception(f"Failed to read scanner: {response.status_code}")
    
    def delete_scanner(self, scanner_url):
        """Delete scanner"""
        requests.delete(
            scanner_url, 
            auth=(self.user, self.password), 
            headers={'Accept': 'application/json'}, 
            verify=False
        )
    
    def scan_full_table(self, table_path, scanner_filter):
        """Scan entire table and return all rows"""
        print(f"Creating scanner for table: {table_path}")
        scanner_url = self.create_scanner(table_path, scanner_filter)
        print(f"Scanner created: {scanner_url}")
        
        all_rows = []
        batch_count = 0
        
        while True:
            batch_count += 1
            data = self.read_scanner(scanner_url)
            
            if data is None or 'Row' not in data:
                break
            
            rows = data['Row']
            all_rows.extend(rows)
            print(f"  Batch {batch_count}: Read {len(rows)} rows (total: {len(all_rows)})")
        
        self.delete_scanner(scanner_url)
        print(f"Scanner deleted. Total rows read: {len(all_rows)}")
        
        return all_rows
    
    def insert_rows(self, table_path, rows_json):
        """Insert rows into table"""
        url = f"{self.url}/{table_path}/dummyrowkey"
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        
        response = requests.put(
            url, 
            data=rows_json, 
            auth=(self.user, self.password), 
            headers=headers, 
            verify=False
        )
        
        if response.status_code == 200:
            return True
        else:
            raise Exception(f"Failed to insert rows: {response.status_code} - {response.text}")
    
    def delete_rows_by_prefix(self, table_path, row_key_prefix):
        """Delete all rows with a given prefix"""
        print(f"  Deleting old leaderboard data with prefix: {row_key_prefix}")
        
        prefix_b64 = base64.b64encode(row_key_prefix.encode()).decode()
        scanner_filter = f'<Scanner batch="{SCANNER_BATCH_SIZE}"><filter>{{"type":"PrefixFilter","value":"{prefix_b64}"}}</filter></Scanner>'
        
        rows_to_delete = self.scan_full_table(table_path, scanner_filter)
        
        if not rows_to_delete:
            print(f"  No existing leaderboard to delete")
            return
        
        print(f"  Deleting {len(rows_to_delete)} old leaderboard entries...")
        
        for row in rows_to_delete:
            row_key = base64.b64decode(row['key']).decode('utf-8')
            encoded_key = row_key.replace('/', '%2F').replace(':', '%3A')
            url = f"{self.url}/{table_path}/{encoded_key}"
            
            response = requests.delete(
                url,
                auth=(self.user, self.password),
                verify=False
            )
            
            if response.status_code not in [200, 204]:
                print(f"  Warning: Failed to delete row {row_key}: {response.status_code}")
        
        print(f"  Successfully deleted old leaderboard")


# ============================================================================
# Data Processing Functions
# ============================================================================

def decode_row(row):
    """Decode a row from HBase format to dictionary"""
    row_dict = {}
    row_dict['_row_key'] = base64.b64decode(row['key']).decode('utf-8')
    
    for cell in row.get('Cell', []):
        column = base64.b64decode(cell['column']).decode('utf-8')
        value = base64.b64decode(cell['$']).decode('utf-8', errors='replace')
        row_dict[column] = value
    
    return row_dict


def parse_numeric(value, default=0):
    """Safely parse numeric value"""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def group_by_track_and_uuid(rows):
    """
    Group rows by track and UUID
    Returns: {track_id: {uuid: [rows]}}
    """
    print("\nGrouping data by track and UUID...")
    
    grouped = defaultdict(lambda: defaultdict(list))
    
    for row in rows:
        decoded = decode_row(row)
        
        track_id = decoded.get('telemetry:TrackID', 'unknown')
        uuid = decoded.get('telemetry:uuid', 'unknown')
        
        grouped[track_id][uuid].append(decoded)
    
    print(f"Found {len(grouped)} unique tracks:")
    for track_id, uuids in grouped.items():
        print(f"  Track '{track_id}': {len(uuids)} sessions")
    
    return grouped


def find_all_valid_laps(track_id, sessions_data):
    """
    Find ALL valid completed laps for a track across all sessions
    
    Returns:
    --------
    list of tuples: [(uuid, lap_num, lap_time, metadata), ...]
    """
    print(f"\n  Analyzing track: {track_id}")
    
    all_valid_laps = []
    
    for uuid, session_rows in sessions_data.items():
        print(f"    Session {uuid}: {len(session_rows)} rows")
        
        # Group by lap number
        laps = defaultdict(list)
        for row in session_rows:
            lap_num = parse_numeric(row.get('telemetry:Lap'), default=None)
            if lap_num is not None:
                laps[int(lap_num)].append(row)
        
        print(f"      Found {len(laps)} laps in this session")
        
        # Extract metadata once for this session
        session_metadata = {}
        for row in session_rows:
            metadata_cols = {k: v for k, v in row.items() if k.startswith('metadata:') or k == 'telemetry:source_file'}
            if metadata_cols:
                session_metadata = metadata_cols
                break
        
        # Analyze each lap
        for lap_num, lap_rows in laps.items():
            if len(lap_rows) < MIN_DATA_POINTS:
                continue
            
            # Try to find completed lap time
            lap_time = None
            
            # Method 1: Check LapLastLapTime in next lap
            next_lap_rows = laps.get(lap_num + 1, [])
            for row in next_lap_rows:
                last_lap_time = parse_numeric(row.get('telemetry:LapLastLapTime'), default=0)
                if MIN_LAP_TIME < last_lap_time < MAX_LAP_TIME:
                    lap_time = last_lap_time
                    break
            
            # Method 2: Use maximum LapCurrentLapTime
            if lap_time is None:
                max_current_time = max([parse_numeric(r.get('telemetry:LapCurrentLapTime'), default=0) for r in lap_rows])
                if MIN_LAP_TIME < max_current_time < MAX_LAP_TIME:
                    lap_time = max_current_time
            
            # If valid lap time found, add to list
            if lap_time is not None:
                all_valid_laps.append((uuid, lap_num, lap_time, session_metadata))
                print(f"      Valid lap: Lap {lap_num} = {lap_time:.3f}s")
    
    return all_valid_laps


def create_leaderboard_rows(track_id, top_laps):
    """
    Create HBase rows for the leaderboard table
    
    Row key format: <track_id>:<rank_padded>:<uuid>
    """
    print(f"  Creating leaderboard rows for track {track_id}...")
    
    hbase_rows = []
    
    for rank, (uuid, lap_num, lap_time, metadata) in enumerate(top_laps, start=1):
        # Create row key with zero-padded rank
        row_key = f"{track_id}:{rank:02d}:{uuid}"
        row_key_b64 = base64.b64encode(row_key.encode()).decode()
        
        # Extract metadata fields
        driver_name = metadata.get('metadata:DriverInfo_Username', 'Unknown')
        car_model = metadata.get('metadata:DriverInfo_Drivers_CarScreenName', 'Unknown')
        car_number = metadata.get('metadata:DriverInfo_Drivers_CarNumber', 'Unknown')
        track_name = metadata.get('metadata:WeekendInfo_TrackDisplayName', 'Unknown')
        date_recorded = metadata.get('metadata:WeekendInfo_WeekendOptions_Date', 'Unknown')
        source_file = metadata.get('telemetry:source_file', 'Unknown')
        
        # Build HBase row
        hbase_row = {
            'key': row_key_b64,
            'Cell': []
        }
        
        # Add lap_info columns
        lap_info_fields = {
            'lap_info:lap_time': str(lap_time),
            'lap_info:lap_number': str(lap_num),
            'lap_info:date_recorded': date_recorded,
            'lap_info:track_name': track_name,
            'lap_info:rank': str(rank)
        }
        
        for col_name, value in lap_info_fields.items():
            col_b64 = base64.b64encode(col_name.encode()).decode()
            val_b64 = base64.b64encode(str(value).encode()).decode()
            hbase_row['Cell'].append({
                'column': col_b64,
                '$': val_b64
            })
        
        # Add driver_info columns
        driver_info_fields = {
            'driver_info:driver_name': driver_name,
            'driver_info:car_model': car_model,
            'driver_info:car_number': car_number
        }
        
        for col_name, value in driver_info_fields.items():
            col_b64 = base64.b64encode(col_name.encode()).decode()
            val_b64 = base64.b64encode(str(value).encode()).decode()
            hbase_row['Cell'].append({
                'column': col_b64,
                '$': val_b64
            })
        
        # Add telemetry_ref columns
        telemetry_ref_fields = {
            'telemetry_ref:uuid': str(uuid),
            'telemetry_ref:source_file': source_file
        }
        
        for col_name, value in telemetry_ref_fields.items():
            col_b64 = base64.b64encode(col_name.encode()).decode()
            val_b64 = base64.b64encode(str(value).encode()).decode()
            hbase_row['Cell'].append({
                'column': col_b64,
                '$': val_b64
            })
        
        hbase_rows.append(hbase_row)
        
        print(f"    #{rank}: {driver_name} - {lap_time:.3f}s (Lap {lap_num}, UUID: {uuid})")
    
    return hbase_rows


# ============================================================================
# Main Processing
# ============================================================================

def main():
    print("="*70)
    print("LEADERBOARD COMPUTATION JOB")
    print("="*70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Finding top {TOP_N_LAPS} fastest laps per track")
    print()
    
    # Initialize HBase client
    print("Initializing HBase REST client...")
    client = HBaseRestClient(
        HBASE_USER,
        HBASE_PASSWORD,
        HBASE_REST_NODE,
        HBASE_REST_NODE_IP,
        HBASE_REST_PORT
    )
    
    # Encode table paths
    main_table = MAIN_TABLE_PATH.replace('/', '%2F')
    leaderboard_table = LEADERBOARD_TABLE_PATH.replace('/', '%2F')
    
    # Build column filter
    column_filters = ''.join([
        f'<column>{base64.b64encode(col.encode()).decode()}</column>'
        for col in MINIMAL_COLUMNS
    ])
    
    scanner_filter = f'<Scanner batch="{SCANNER_BATCH_SIZE}"><filter/>{column_filters}</Scanner>'
    
    # Step 1: Scan main table with minimal columns
    print("\n" + "="*70)
    print("STEP 1: Scanning main table with minimal columns")
    print("="*70)
    
    try:
        all_rows = client.scan_full_table(main_table, scanner_filter)
        
        if not all_rows:
            print("No data found in main table. Exiting.")
            return
        
        print(f"\n✓ Successfully read {len(all_rows)} rows from main table")
    
    except Exception as e:
        print(f"\n✗ Error scanning main table: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    # Step 2: Group by track and UUID
    print("\n" + "="*70)
    print("STEP 2: Organizing data by track and session")
    print("="*70)
    
    grouped_data = group_by_track_and_uuid(all_rows)
    
    # Step 3: Find all valid laps and create leaderboards
    print("\n" + "="*70)
    print("STEP 3: Finding all valid laps and ranking them")
    print("="*70)
    
    track_leaderboards = {}
    
    for track_id, sessions_data in grouped_data.items():
        if track_id == 'unknown':
            print(f"\n  Skipping track: {track_id} (invalid data)")
            continue
        
        # Get all valid laps for this track
        all_valid_laps = find_all_valid_laps(track_id, sessions_data)
        
        if not all_valid_laps:
            print(f"  ✗ No valid laps found for track '{track_id}'")
            continue
        
        # Sort by lap time (ascending) and take top N
        all_valid_laps.sort(key=lambda x: x[2])  # Sort by lap_time (index 2)
        top_laps = all_valid_laps[:TOP_N_LAPS]
        
        print(f"  ✓ Found {len(all_valid_laps)} valid laps for track '{track_id}'")
        print(f"  ✓ Top {len(top_laps)} laps selected for leaderboard")
        
        track_leaderboards[track_id] = top_laps
    
    # Step 4: Write leaderboards to table
    print("\n" + "="*70)
    print("STEP 4: Writing leaderboards to table")
    print("="*70)
    
    for track_id, top_laps in track_leaderboards.items():
        print(f"\nProcessing leaderboard for track: {track_id}")
        
        try:
            # Delete old leaderboard for this track
            row_prefix = f"{track_id}:"
            client.delete_rows_by_prefix(leaderboard_table, row_prefix)
            
            # Create new leaderboard rows
            hbase_rows = create_leaderboard_rows(track_id, top_laps)
            
            # Convert to JSON
            rows_json = json.dumps({'Row': hbase_rows})
            
            # Insert into leaderboard table
            print(f"  Inserting {len(hbase_rows)} leaderboard entries...")
            client.insert_rows(leaderboard_table, rows_json)
            
            print(f"  ✓ Successfully wrote leaderboard for track '{track_id}'")
        
        except Exception as e:
            print(f"  ✗ Error writing leaderboard for track '{track_id}': {e}")
            import traceback
            traceback.print_exc()
    
    # Summary
    print("\n" + "="*70)
    print("JOB COMPLETE")
    print("="*70)
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Created leaderboards for {len(track_leaderboards)} tracks")
    
    print("\nLeaderboard Summary:")
    for track_id, top_laps in track_leaderboards.items():
        print(f"\n  Track {track_id}:")
        for rank, (uuid, lap_num, lap_time, metadata) in enumerate(top_laps, start=1):
            driver = metadata.get('metadata:DriverInfo_Username', 'Unknown')
            print(f"    #{rank}: {driver} - {lap_time:.3f}s")
    print()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nJob interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)