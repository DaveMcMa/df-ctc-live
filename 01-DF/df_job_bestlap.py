### this version deletes the table and recreates if new best laps are found!
#!/usr/bin/env python3
"""
Best Lap Computation Job

This script scans the main telemetry table (/ctc/ctc-table), identifies the 
fastest lap for each track, and stores it in the best lap table (/ctc/bestlap-table).

When a new faster lap is found, it overwrites the previous best lap for that track.

Usage:
    python bestlap_compute.py
"""

import sys
import base64
import json
import requests
import urllib3
import subprocess
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
BESTLAP_TABLE_PATH = '/ctc/bestlap-table'

# Scanning
SCANNER_BATCH_SIZE = 100000  # Increased for better performance

# Column Selection for First Pass (finding best laps)
MINIMAL_COLUMNS = [
    'telemetry:Lap',
    'telemetry:LapLastLapTime',
    'telemetry:LapCurrentLapTime',
    'telemetry:uuid',
    'telemetry:TrackID',
    'telemetry:SessionTick'
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
            # No more data
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
    
    def scan_with_uuid_filter(self, table_path, uuid_value):
        """
        Scan table for all rows with a specific UUID
        This gets the full telemetry data for a specific session
        """
        print(f"  Scanning for UUID: {uuid_value}")
        
        # Create filter for this UUID
        uuid_col = base64.b64encode(b'telemetry:uuid').decode()
        uuid_val = base64.b64encode(uuid_value.encode()).decode()
        
        scanner_filter = f'''<Scanner batch="{SCANNER_BATCH_SIZE}">
            <filter>
                {{
                    "type": "SingleColumnValueFilter",
                    "op": "EQUAL",
                    "family": "{base64.b64encode(b'telemetry').decode()}",
                    "qualifier": "{base64.b64encode(b'uuid').decode()}",
                    "comparator": {{
                        "type": "BinaryComparator",
                        "value": "{uuid_val}"
                    }}
                }}
            </filter>
        </Scanner>'''
        
        rows = self.scan_full_table(table_path, scanner_filter)
        print(f"  Found {len(rows)} rows for UUID {uuid_value}")
        
        return rows
    
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
    
    def recreate_table_for_track(self, table_path_raw, track_id):
        """
        Fast table recreation using MapR CLI instead of slow row-by-row deletion.
        Only recreates if we're updating data for a track.
        
        WARNING: This deletes ALL data in the table, not just one track's data.
        Only use when you have a single track or want to refresh everything.
        """
        print(f"  Using MapR CLI to quickly recreate table (much faster than row-by-row delete)")
        
        try:
            # Delete the table
            print(f"  Deleting table: {table_path_raw}")
            result = subprocess.run(
                ['maprcli', 'table', 'delete', '-path', table_path_raw],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                # Table might not exist, which is fine
                print(f"  Table delete result: {result.stderr.strip()}")
            else:
                print(f"  Table deleted successfully")
            
            # Recreate the table
            print(f"  Creating table: {table_path_raw}")
            result = subprocess.run(
                ['maprcli', 'table', 'create', '-path', table_path_raw, '-tabletype', 'binary'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                raise Exception(f"Failed to create table: {result.stderr}")
            
            print(f"  Table created successfully")
            
            # Create column families
            for cf in ['telemetry', 'metadata', 'bestlap_summary']:
                print(f"  Creating column family: {cf}")
                result = subprocess.run(
                    ['maprcli', 'table', 'cf', 'create', '-path', table_path_raw, '-cfname', cf],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if result.returncode != 0:
                    raise Exception(f"Failed to create column family {cf}: {result.stderr}")
            
            print(f"  ✓ Table recreated successfully with all column families")
            return True
            
        except subprocess.TimeoutExpired:
            raise Exception("MapR CLI command timed out")
        except FileNotFoundError:
            raise Exception("maprcli command not found - ensure MapR CLI is installed and in PATH")
        except Exception as e:
            raise Exception(f"Error recreating table: {e}")
    
    def delete_rows_by_prefix(self, table_path, row_key_prefix):
        """Delete all rows with a given prefix (to overwrite old best lap)"""
        print(f"  Deleting old best lap data with prefix: {row_key_prefix}")
        
        # Scan for rows with this prefix
        prefix_b64 = base64.b64encode(row_key_prefix.encode()).decode()
        scanner_filter = f'<Scanner batch="{SCANNER_BATCH_SIZE}"><filter>{{"type":"PrefixFilter","value":"{prefix_b64}"}}</filter></Scanner>'
        
        rows_to_delete = self.scan_full_table(table_path, scanner_filter)
        
        if not rows_to_delete:
            print(f"  No existing data to delete")
            return
        
        print(f"  Deleting {len(rows_to_delete)} old rows...")
        
        # Delete each row individually
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
        
        print(f"  Successfully deleted old best lap data")


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
        
        # Extract track ID and UUID
        track_id = decoded.get('telemetry:TrackID', 'unknown')
        uuid = decoded.get('telemetry:uuid', 'unknown')
        
        grouped[track_id][uuid].append(decoded)
    
    print(f"Found {len(grouped)} unique tracks:")
    for track_id, uuids in grouped.items():
        print(f"  Track '{track_id}': {len(uuids)} sessions")
    
    return grouped


def find_best_lap_for_track(track_id, sessions_data):
    """
    Find the absolute fastest lap for a given track across all sessions
    
    Parameters:
    -----------
    track_id : str
        The track identifier
    sessions_data : dict
        {uuid: [rows]} - all sessions for this track
    
    Returns:
    --------
    tuple : (best_uuid, best_lap_num, best_lap_time, best_lap_rows, metadata)
    """
    print(f"\n  Analyzing track: {track_id}")
    
    best_lap_info = None
    best_lap_time = float('inf')
    best_lap_rows = []
    best_metadata = {}
    
    # Analyze each session (UUID)
    for uuid, session_rows in sessions_data.items():
        print(f"    Session {uuid}: {len(session_rows)} rows")
        
        # Group by lap number within this session
        laps = defaultdict(list)
        for row in session_rows:
            lap_num = parse_numeric(row.get('telemetry:Lap'), default=None)
            if lap_num is not None:
                laps[int(lap_num)].append(row)
        
        print(f"      Found {len(laps)} laps in this session")
        
        # Analyze each lap
        for lap_num, lap_rows in laps.items():
            if len(lap_rows) < MIN_DATA_POINTS:
                continue
            
            # Try to find completed lap time
            lap_time = None
            
            # Method 1: Check LapLastLapTime in the next lap
            next_lap_rows = laps.get(lap_num + 1, [])
            for row in next_lap_rows:
                last_lap_time = parse_numeric(row.get('telemetry:LapLastLapTime'), default=0)
                if MIN_LAP_TIME < last_lap_time < MAX_LAP_TIME:
                    lap_time = last_lap_time
                    break
            
            # Method 2: Use maximum LapCurrentLapTime in current lap
            if lap_time is None:
                max_current_time = max([parse_numeric(r.get('telemetry:LapCurrentLapTime'), default=0) for r in lap_rows])
                if MIN_LAP_TIME < max_current_time < MAX_LAP_TIME:
                    lap_time = max_current_time
            
            # If we found a valid lap time and it's better than current best
            if lap_time is not None and lap_time < best_lap_time:
                best_lap_time = lap_time
                best_lap_info = (uuid, lap_num)
                best_lap_rows = lap_rows
                
                # Extract metadata from first row
                for row in lap_rows:
                    # Look for metadata columns
                    metadata_cols = {k: v for k, v in row.items() if k.startswith('metadata:')}
                    if metadata_cols:
                        best_metadata = metadata_cols
                        break
                
                print(f"      New best lap found! Lap {lap_num}: {lap_time:.3f}s ({len(lap_rows)} points)")
    
    if best_lap_info:
        best_uuid, best_lap_num = best_lap_info
        print(f"  ✓ Best lap for track '{track_id}': UUID {best_uuid}, Lap {best_lap_num}, Time {best_lap_time:.3f}s")
        return best_uuid, best_lap_num, best_lap_time, best_lap_rows, best_metadata
    else:
        print(f"  ✗ No valid laps found for track '{track_id}'")
        return None, None, None, [], {}


def create_bestlap_rows(track_id, uuid, lap_num, lap_time, lap_rows, metadata):
    """
    Create HBase rows for the best lap table
    
    Row key format: <track_id>:<uuid>:<row_index>
    """
    print(f"  Creating {len(lap_rows)} HBase rows for best lap table...")
    
    hbase_rows = []
    
    # Sort rows by SessionTick if available
    sorted_rows = sorted(lap_rows, key=lambda r: parse_numeric(r.get('telemetry:SessionTick'), default=0))
    
    for idx, row in enumerate(sorted_rows):
        # Create row key
        row_key = f"{track_id}:{uuid}:{idx}"
        row_key_b64 = base64.b64encode(row_key.encode()).decode()
        
        # Start building the row
        hbase_row = {
            'key': row_key_b64,
            'Cell': []
        }
        
        # Add all telemetry columns
        for col_name, value in row.items():
            if col_name.startswith('telemetry:'):
                col_b64 = base64.b64encode(col_name.encode()).decode()
                val_b64 = base64.b64encode(str(value).encode()).decode()
                hbase_row['Cell'].append({
                    'column': col_b64,
                    '$': val_b64
                })
        
        # Add metadata columns (only in first row to save space)
        if idx == 0:
            for col_name, value in metadata.items():
                if col_name.startswith('metadata:'):
                    col_b64 = base64.b64encode(col_name.encode()).decode()
                    val_b64 = base64.b64encode(str(value).encode()).decode()
                    hbase_row['Cell'].append({
                        'column': col_b64,
                        '$': val_b64
                    })
            
            # Add best lap summary information
            summary_fields = {
                'bestlap_summary:lap_time': str(lap_time),
                'bestlap_summary:lap_number': str(lap_num),
                'bestlap_summary:uuid': str(uuid),
                'bestlap_summary:track_id': str(track_id),
                'bestlap_summary:track_name': metadata.get('metadata:WeekendInfo_TrackDisplayName', 'Unknown'),
                'bestlap_summary:driver_name': metadata.get('metadata:DriverInfo_Username', 'Unknown'),
                'bestlap_summary:date_recorded': metadata.get('metadata:WeekendInfo_WeekendOptions_Date', 'Unknown'),
                'bestlap_summary:data_points': str(len(lap_rows)),
                'bestlap_summary:computed_date': datetime.now().isoformat()
            }
            
            for col_name, value in summary_fields.items():
                col_b64 = base64.b64encode(col_name.encode()).decode()
                val_b64 = base64.b64encode(str(value).encode()).decode()
                hbase_row['Cell'].append({
                    'column': col_b64,
                    '$': val_b64
                })
        
        hbase_rows.append(hbase_row)
    
    return hbase_rows


# ============================================================================
# Main Processing
# ============================================================================

def main():
    print("="*70)
    print("BEST LAP COMPUTATION JOB (OPTIMIZED)")
    print("="*70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
    bestlap_table = BESTLAP_TABLE_PATH.replace('/', '%2F')
    
    # Define scanner filter with minimal columns for first pass
    print("\n" + "="*70)
    print("OPTIMIZATION: Two-Pass Approach")
    print("  Pass 1: Scan minimal columns to find best laps (FAST)")
    print("  Pass 2: Get full telemetry only for best laps (TARGETED)")
    print("="*70)
    
    # Build column filter for minimal scan
    column_filters = ''.join([
        f'<column>{base64.b64encode(col.encode()).decode()}</column>'
        for col in MINIMAL_COLUMNS
    ])
    
    scanner_filter = f'<Scanner batch="{SCANNER_BATCH_SIZE}"><filter/>{column_filters}</Scanner>'
    
    # Step 1: Scan main table with minimal columns
    print("\n" + "="*70)
    print("STEP 1: Fast scan with minimal columns")
    print("="*70)
    print(f"Scanning only these columns: {', '.join(MINIMAL_COLUMNS)}")
    
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
    
    # Step 3: Find best lap for each track (using minimal data)
    print("\n" + "="*70)
    print("STEP 3: Finding fastest lap for each track")
    print("="*70)
    
    best_laps_minimal = {}
    
    for track_id, sessions_data in grouped_data.items():
        result = find_best_lap_for_track(track_id, sessions_data)
        uuid, lap_num, lap_time, lap_rows, metadata = result
        
        if uuid is not None:
            best_laps_minimal[track_id] = {
                'uuid': uuid,
                'lap_num': lap_num,
                'lap_time': lap_time,
                'row_count': len(lap_rows)
            }
    
    print(f"\n✓ Found best laps for {len(best_laps_minimal)} tracks")
    
    # Step 4: Get full telemetry data for best laps only
    print("\n" + "="*70)
    print("STEP 4: Fetching full telemetry for best laps")
    print("="*70)
    
    best_laps_full = {}
    
    for track_id, lap_info in best_laps_minimal.items():
        print(f"\nFetching full data for track: {track_id}")
        print(f"  UUID: {lap_info['uuid']}")
        print(f"  Lap: {lap_info['lap_num']}")
        print(f"  Time: {lap_info['lap_time']:.3f}s")
        
        try:
            # Get ALL rows for this UUID (full telemetry)
            full_session_rows = client.scan_with_uuid_filter(main_table, lap_info['uuid'])
            
            if not full_session_rows:
                print(f"  ✗ No data found for UUID {lap_info['uuid']}")
                continue
            
            # Decode rows and extract the specific lap
            decoded_rows = [decode_row(row) for row in full_session_rows]
            
            # Filter to just the best lap number
            lap_rows = [
                row for row in decoded_rows
                if parse_numeric(row.get('telemetry:Lap')) == lap_info['lap_num']
            ]
            
            if not lap_rows:
                print(f"  ✗ No rows found for lap {lap_info['lap_num']}")
                continue
            
            # Extract metadata (look through all rows for this UUID)
            metadata = {}
            for row in decoded_rows:
                metadata_cols = {k: v for k, v in row.items() if k.startswith('metadata:')}
                if metadata_cols:
                    metadata = metadata_cols
                    break
            
            print(f"  ✓ Retrieved {len(lap_rows)} telemetry rows")
            
            best_laps_full[track_id] = {
                'uuid': lap_info['uuid'],
                'lap_num': lap_info['lap_num'],
                'lap_time': lap_info['lap_time'],
                'lap_rows': lap_rows,
                'metadata': metadata
            }
        
        except Exception as e:
            print(f"  ✗ Error fetching full data for track '{track_id}': {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n✓ Retrieved full telemetry for {len(best_laps_full)} tracks")
    
    # Step 5: Write to best lap table
    print("\n" + "="*70)
    print("STEP 5: Writing best laps to best lap table")
    print("="*70)
    
    # Check if ANY track has changed data
    any_changes = False
    tracks_to_update = []
    
    for track_id, lap_data in best_laps_full.items():
        print(f"\nChecking track: {track_id}")
        print(f"  Best lap: {lap_data['lap_time']:.3f}s by {lap_data['metadata'].get('metadata:DriverInfo_Username', 'Unknown')}")
        
        try:
            # Check if this lap is already in the table
            row_prefix = f"{track_id}:"
            prefix_b64 = base64.b64encode(row_prefix.encode()).decode()
            check_filter = f'<Scanner batch="1"><filter>{{"type":"PrefixFilter","value":"{prefix_b64}"}}</filter><column>{base64.b64encode(b"bestlap_summary:uuid").decode()}</column><column>{base64.b64encode(b"bestlap_summary:lap_time").decode()}</column></Scanner>'
            
            existing_rows = client.scan_full_table(bestlap_table, check_filter)
            
            # If we have existing data, check if it's the same
            if existing_rows:
                existing_row = decode_row(existing_rows[0])
                existing_uuid = existing_row.get('bestlap_summary:uuid', '')
                existing_lap_time = existing_row.get('bestlap_summary:lap_time', '')
                
                if existing_uuid == lap_data['uuid'] and existing_lap_time == str(lap_data['lap_time']):
                    print(f"  ⏭ Best lap unchanged - will skip")
                else:
                    print(f"  🔄 New best lap detected!")
                    any_changes = True
                    tracks_to_update.append(track_id)
            else:
                print(f"  🆕 New track - will add")
                any_changes = True
                tracks_to_update.append(track_id)
                
        except Exception as e:
            print(f"  ⚠️ Error checking track: {e}")
            any_changes = True
            tracks_to_update.append(track_id)
    
    # If any changes detected, recreate the entire table (much faster than row-by-row delete)
    if any_changes:
        print(f"\n{'='*70}")
        print(f"DETECTED CHANGES - Recreating table for fast update")
        print(f"  Tracks to update: {', '.join(tracks_to_update)}")
        print(f"{'='*70}")
        
        try:
            client.recreate_table_for_track(BESTLAP_TABLE_PATH, None)
        except Exception as e:
            print(f"\n⚠️ Warning: Could not recreate table via MapR CLI: {e}")
            print(f"Falling back to row-by-row deletion (this will be slow)...")
    else:
        print(f"\n{'='*70}")
        print(f"NO CHANGES DETECTED - All best laps are up to date!")
        print(f"{'='*70}")
    
    # Now insert data for tracks that need updating
    for track_id, lap_data in best_laps_full.items():
        if track_id not in tracks_to_update:
            continue
        
        print(f"\nInserting data for track: {track_id}")
        
        try:
            # If table recreation failed, delete old rows manually
            if any_changes and track_id in tracks_to_update:
                # Table was just recreated, so no need to delete
                # But if recreation failed, we need to delete manually
                pass
            
            # Create new best lap rows
            hbase_rows = create_bestlap_rows(
                track_id,
                lap_data['uuid'],
                lap_data['lap_num'],
                lap_data['lap_time'],
                lap_data['lap_rows'],
                lap_data['metadata']
            )
            
            # Convert to JSON format for HBase REST API
            rows_json = json.dumps({'Row': hbase_rows})
            
            # Insert into best lap table
            print(f"  Inserting {len(hbase_rows)} rows into best lap table...")
            client.insert_rows(bestlap_table, rows_json)
            
            print(f"  ✓ Successfully wrote best lap for track '{track_id}'")
        
        except Exception as e:
            print(f"  ✗ Error writing best lap for track '{track_id}': {e}")
            import traceback
            traceback.print_exc()
    
    # Summary
    print("\n" + "="*70)
    print("JOB COMPLETE")
    print("="*70)
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Processed {len(all_rows)} rows in minimal scan")
    print(f"Found best laps for {len(best_laps_full)} tracks")
    print("\nBest laps summary:")
    for track_id, lap_data in best_laps_full.items():
        print(f"  {track_id}: {lap_data['lap_time']:.3f}s - {lap_data['metadata'].get('metadata:DriverInfo_Username', 'Unknown')} ({len(lap_data['lap_rows'])} data points)")
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