import streamlit as st
import pandas as pd
import requests
import urllib3
import base64
import time
import json
import numpy as np
from datetime import datetime
import sys
import pydeck as pdk
import plotly.graph_objects as go

# ============================================================================
# CONFIGURATION - Edit these values
# ============================================================================
# HBase Connection Configuration
HBASE_USER = 'mapr'
HBASE_PASSWORD = 'mapr123'
HBASE_REST_NODE_IP = '10.1.84.212'
HBASE_REST_NODE = 'ezdf-core3.ezmeral.demo.local'
HBASE_REST_PORT = '8080'

# Tables
BESTLAP_TABLE_PATH = "/ctc/bestlap-table"
LEADERBOARD_TABLE_PATH = "/ctc/leaderboard-table"

# Scanner Configuration
SCANNER_BATCH_SIZE = 100000

# Replay Configuration
ROWS_PER_SECOND = 60  # Speed of telemetry replay
ROW_SLEEP_TIME_MULTIPLIER = 0.5  # Multiplier for sleep time calculation

# Visualization Configuration
MAX_HISTORY_POINTS = 50  # Maximum number of points to keep in history for charts
CHART_UPDATE_FREQUENCY = 3  # Update charts every N iterations
MAP_UPDATE_FREQUENCY_DIVISOR = 30  # Map updates per second

# Map Configuration
MAP_STYLE = "mapbox://styles/mapbox/satellite-v9"
MAP_INITIAL_ZOOM = 15
MAP_INITIAL_PITCH = 0
MAP_POINT_RADIUS = 7
MAP_POINT_COLOR = [255, 0, 0, 200]  # Red RGBA

# Chart Configuration - Y-axis ranges
THROTTLE_BRAKE_Y_RANGE = [0, 100]
STEERING_Y_RANGE = [-10, 10]
SPEED_Y_RANGE = [0, 250]
RPM_Y_RANGE = [0, 15000]
CHART_HEIGHT_SMALL = 150
CHART_HEIGHT_MEDIUM = 180

# Restart Configuration
RESTART_DELAY_SECONDS = 3  # Time to wait before restarting after completion
# ============================================================================

# Disable SSL warnings
try:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except AttributeError:
    # Older versions of urllib3
    try:
        urllib3.disable_warnings()
    except:
        pass


class HBaseRest:
    def __init__(self, user, password, rest_node, rest_node_ip, rest_node_port):
        self.user = user
        self.password = password
        self.host = rest_node
        self.ip = rest_node_ip
        self.port = rest_node_port
        self.url = f"https://{rest_node}:{rest_node_port}"
    
    def create_scanner(self, table, filter_xml):
        """Create a scanner for the specified table"""
        url = f"{self.url}/{table}/scanner"
        headers = {'Accept': 'application/xml', 'Content-Type': 'text/xml'}
        
        response = requests.put(
            url, 
            data=filter_xml, 
            auth=(self.user, self.password), 
            headers=headers, 
            verify=False
        )
        
        if response.status_code == 201:
            return response.headers.get("Location")
        else:
            raise Exception(f"Failed to create scanner: {response.status_code}")

    def read_scanner(self, scanner_url):
        """Read data from the scanner"""
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
        """Delete the scanner"""
        requests.delete(
            scanner_url, 
            auth=(self.user, self.password), 
            headers={'Accept': 'application/json'}, 
            verify=False
        )
    
    def scan_table(self, table_path, scanner_filter):
        """Scan table and return all rows"""
        scanner_url = self.create_scanner(table_path, scanner_filter)
        
        all_rows = []
        while True:
            data = self.read_scanner(scanner_url)
            if data is None or 'Row' not in data:
                break
            all_rows.extend(data['Row'])
        
        self.delete_scanner(scanner_url)
        return all_rows


def decode_row(row):
    """Decode HBase row to dictionary"""
    row_dict = {}
    row_dict['_row_key'] = base64.b64decode(row['key']).decode('utf-8')
    
    for cell in row.get('Cell', []):
        column = base64.b64decode(cell['column']).decode('utf-8')
        value = base64.b64decode(cell['$']).decode('utf-8', errors='replace')
        row_dict[column] = value
    
    return row_dict


def get_available_tracks(hbase_client):
    """Get list of available tracks from bestlap table"""
    table_path = BESTLAP_TABLE_PATH.replace('/', '%2F')
    
    # Scan for bestlap_summary:track_id column only
    track_col = base64.b64encode(b'bestlap_summary:track_id').decode()
    track_name_col = base64.b64encode(b'bestlap_summary:track_name').decode()
    
    scanner_filter = f'''<Scanner batch="{SCANNER_BATCH_SIZE}">
        <filter/>
        <column>{track_col}</column>
        <column>{track_name_col}</column>
    </Scanner>'''
    
    rows = hbase_client.scan_table(table_path, scanner_filter)
    
    # Extract unique tracks
    tracks = {}
    for row in rows:
        decoded = decode_row(row)
        track_id = decoded.get('bestlap_summary:track_id', 'unknown')
        track_name = decoded.get('bestlap_summary:track_name', f'Track {track_id}')
        
        if track_id not in tracks:
            tracks[track_id] = track_name
    
    return tracks


def fetch_best_lap_data(hbase_client, track_id):
    """Fetch best lap telemetry for a specific track"""
    table_path = BESTLAP_TABLE_PATH.replace('/', '%2F')
    
    # Scan with prefix filter for this track
    prefix = f"{track_id}:"
    prefix_b64 = base64.b64encode(prefix.encode()).decode()
    
    scanner_filter = f'''<Scanner batch="{SCANNER_BATCH_SIZE}">
        <filter>{{"type":"PrefixFilter","value":"{prefix_b64}"}}</filter>
    </Scanner>'''
    
    rows = hbase_client.scan_table(table_path, scanner_filter)
    
    # Decode all rows
    decoded_rows = [decode_row(row) for row in rows]
    
    # Convert to DataFrame
    df = pd.DataFrame(decoded_rows)
    
    return df


def fetch_leaderboard(hbase_client, track_id):
    """Fetch leaderboard for a specific track"""
    table_path = LEADERBOARD_TABLE_PATH.replace('/', '%2F')
    
    # Scan with prefix filter for this track
    prefix = f"{track_id}:"
    prefix_b64 = base64.b64encode(prefix.encode()).decode()
    
    scanner_filter = f'''<Scanner batch="{SCANNER_BATCH_SIZE}">
        <filter>{{"type":"PrefixFilter","value":"{prefix_b64}"}}</filter>
    </Scanner>'''
    
    rows = hbase_client.scan_table(table_path, scanner_filter)
    
    # Decode and extract leaderboard data
    leaderboard = []
    for row in rows:
        decoded = decode_row(row)
        
        # Extract rank from row key (format: trackid:rank:uuid)
        row_key = decoded['_row_key']
        rank = int(row_key.split(':')[1])
        
        leaderboard.append({
            'Rank': rank,
            'Driver': decoded.get('driver_info:driver_name', 'Unknown'),
            'Lap Time': float(decoded.get('lap_info:lap_time', 0)),
            'Car': decoded.get('driver_info:car_model', 'Unknown'),
            'Date': decoded.get('lap_info:date_recorded', 'Unknown')
        })
    
    # Sort by rank
    leaderboard.sort(key=lambda x: x['Rank'])
    
    return pd.DataFrame(leaderboard)


def format_lap_time(seconds):
    """Format lap time as MM:SS.mmm"""
    try:
        seconds = float(seconds)
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes:02d}:{secs:06.3f}"
    except:
        return "Unknown"


def fastest_lap_dashboard():
    st.set_page_config(layout="wide", page_title="Fastest Lap Telemetry Viewer")
    
    st.title("Fastest Lap Telemetry Viewer")
    
    # Initialize HBase client
    hbase_client = HBaseRest(
        HBASE_USER,
        HBASE_PASSWORD,
        HBASE_REST_NODE,
        HBASE_REST_NODE_IP,
        HBASE_REST_PORT
    )
    
    # Get available tracks
    with st.spinner("Loading available tracks..."):
        try:
            tracks = get_available_tracks(hbase_client)
            
            if not tracks:
                st.error("No tracks found in database. Please run bestlap_compute.py first.")
                return
        except Exception as e:
            st.error(f"Error loading tracks: {e}")
            return
    
    # Track selection in sidebar
    st.sidebar.title("Track Selection")
    
    # Create track options with friendly names
    track_options = {f"{name} ({track_id})": track_id for track_id, name in tracks.items()}
    
    selected_track_display = st.sidebar.selectbox(
        "Select Track",
        options=list(track_options.keys())
    )
    
    selected_track_id = track_options[selected_track_display]
    
    # Load button
    if st.sidebar.button("Load Best Lap"):
        st.session_state.load_lap = True
        st.session_state.selected_track = selected_track_id
    
    # Check if we should load a lap
    if 'load_lap' not in st.session_state or not st.session_state.load_lap:
        st.info("Select a track and click 'Load Best Lap' to begin")
        return
    
    track_id = st.session_state.selected_track
    
    # Fetch leaderboard
    with st.spinner("Loading leaderboard..."):
        try:
            leaderboard_df = fetch_leaderboard(hbase_client, track_id)
        except Exception as e:
            st.warning(f"Could not load leaderboard: {e}")
            leaderboard_df = pd.DataFrame()
    
    # Fetch best lap data
    with st.spinner("Loading best lap telemetry..."):
        try:
            df = fetch_best_lap_data(hbase_client, track_id)
            
            if df.empty:
                st.error(f"No best lap data found for track {track_id}")
                return
            
        except Exception as e:
            st.error(f"Error loading best lap data: {e}")
            return
    
    # Extract metadata from first row
    metadata_row = df.iloc[0]
    
    # Get summary info
    driver_name = metadata_row.get('bestlap_summary:driver_name', 'Unknown')
    track_name = metadata_row.get('bestlap_summary:track_name', f'Track {track_id}')
    lap_time = float(metadata_row.get('bestlap_summary:lap_time', 0))
    lap_number = metadata_row.get('bestlap_summary:lap_number', '?')
    date_recorded = metadata_row.get('bestlap_summary:date_recorded', 'Unknown')
    
    formatted_time = format_lap_time(lap_time)
    
    # Create layout
    col1, col2, col3 = st.columns([4, 4, 3])
    
    # Left column - Charts
    with col1:
        st.subheader("Telemetry Graphs")
        throttle_brake_chart = st.empty()
        steering_chart = st.empty()
        speed_rpm_gear_chart = st.empty()
    
    # Middle column - Map
    with col2:
        st.subheader("Live Position Tracking")
        map_container = st.empty()
    
    # Right column - Telemetry data
    with col3:
        st.subheader("Live Telemetry Data")
        telemetry_container = st.empty()
    
    # Sidebar - Leaderboard and Metadata
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"## Current Record")
    st.sidebar.markdown(f"**Driver:** {driver_name}")
    st.sidebar.markdown(f"**Track:** {track_name}")
    st.sidebar.markdown(f"**Lap Time:** {formatted_time}")
    st.sidebar.markdown(f"**Lap Number:** {lap_number}")
    st.sidebar.markdown(f"**Date:** {date_recorded}")
    
    # Display leaderboard
    if not leaderboard_df.empty:
        st.sidebar.markdown("---")
        st.sidebar.markdown("## Top 10 Leaderboard")
        
        # Format lap times for display
        leaderboard_display = leaderboard_df.copy()
        leaderboard_display['Lap Time'] = leaderboard_display['Lap Time'].apply(format_lap_time)
        
        # Set Rank as index to remove the redundant column
        leaderboard_display = leaderboard_display.set_index('Rank')
        
        # Show as table
        st.sidebar.dataframe(
            leaderboard_display[['Driver', 'Lap Time']]
        )
    
    # Display metadata
    st.sidebar.markdown("---")
    st.sidebar.markdown("## Session Details")
    
    metadata_fields = {
        'Car Model': metadata_row.get('metadata:DriverInfo_Drivers_CarScreenName', 'Unknown'),
        'Car Number': metadata_row.get('metadata:DriverInfo_Drivers_CarNumber', 'Unknown'),
        'Track Temperature': metadata_row.get('metadata:WeekendInfo_TrackSurfaceTemp', 'Unknown'),
        'Session Time': metadata_row.get('metadata:WeekendInfo_WeekendOptions_TimeOfDay', 'Unknown'),
        'UUID': metadata_row.get('bestlap_summary:uuid', 'Unknown')
    }
    
    for field, value in metadata_fields.items():
        if value and value != 'Unknown':
            st.sidebar.markdown(f"**{field}:** {value}")
    
    # Sort by session tick for chronological order
    if 'telemetry:SessionTick' in df.columns:
        df['telemetry:SessionTick'] = pd.to_numeric(df['telemetry:SessionTick'], errors='coerce')
        df = df.sort_values('telemetry:SessionTick').reset_index(drop=True)
    
    # Convert numeric columns
    for col in ['telemetry:Lat', 'telemetry:Lon', 'telemetry:Speed', 'telemetry:RPM', 
                'telemetry:Throttle', 'telemetry:Brake', 'telemetry:SteeringWheelAngle',
                'telemetry:Gear', 'telemetry:LapCurrentLapTime']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    total_rows = len(df)
    
    # Initialize visualization history
    throttle_history = []
    brake_history = []
    steering_history = []
    speed_history = []
    rpm_history = []
    gear_history = []
    
    # Initialize map
    point_layer = pdk.Layer(
        'ScatterplotLayer',
        data=[],
        get_position='[lon, lat]',
        get_radius=MAP_POINT_RADIUS,
        get_fill_color=MAP_POINT_COLOR,
        pickable=False,
        auto_highlight=False
    )
    
    initial_view_state = pdk.ViewState(
        latitude=0,
        longitude=0,
        zoom=MAP_INITIAL_ZOOM,
        pitch=MAP_INITIAL_PITCH
    )
    
    deck = pdk.Deck(
        map_style=MAP_STYLE,
        initial_view_state=initial_view_state,
        layers=[point_layer]
    )
    
    map_container.pydeck_chart(deck)
    
    map_initialized = False
    map_update_counter = 0
    map_update_frequency = max(1, int(ROWS_PER_SECOND / MAP_UPDATE_FREQUENCY_DIVISOR))
    
    row_sleep_time = ROW_SLEEP_TIME_MULTIPLIER / ROWS_PER_SECOND
    
    # Replay telemetry
    for i, row in df.iterrows():
        # Extract telemetry values with safe NaN handling
        lap_progress = row.get('telemetry:LapCurrentLapTime', 0)
        if pd.isna(lap_progress):
            lap_progress = 0
            
        speed = row.get('telemetry:Speed', 0)
        if pd.isna(speed):
            speed = 0
        else:
            speed = speed * 3.6  # Convert to km/h
            
        rpm = row.get('telemetry:RPM', 0)
        if pd.isna(rpm):
            rpm = 0
            
        gear_val = row.get('telemetry:Gear', 0)
        if pd.isna(gear_val):
            gear = 0
        else:
            gear = int(gear_val)
            
        throttle = row.get('telemetry:Throttle', 0)
        if pd.isna(throttle):
            throttle = 0
        else:
            throttle = throttle * 100
            
        brake = row.get('telemetry:Brake', 0)
        if pd.isna(brake):
            brake = 0
        else:
            brake = brake * 100
            
        steering = row.get('telemetry:SteeringWheelAngle', 0)
        if pd.isna(steering):
            steering = 0
        
        # Format lap progress
        lap_minutes = int(lap_progress // 60)
        lap_seconds = lap_progress % 60
        formatted_progress = f"{lap_minutes:02d}:{lap_seconds:06.3f}"
        
        # Update telemetry display
        telemetry_md = f"""
        **Lap Time:** {formatted_progress}
        
        **Speed:** {speed:.1f} km/h
        
        **RPM:** {rpm:.0f}
        
        **Gear:** {gear}
        
        **Throttle:** {throttle:.1f}%
        
        **Brake:** {brake:.1f}%
        
        **Steering:** {steering:.2f}°
        """
        telemetry_container.markdown(telemetry_md)
        
        # Add to history
        throttle_history.append(throttle)
        brake_history.append(brake)
        steering_history.append(steering)
        speed_history.append(speed)
        rpm_history.append(rpm)
        gear_history.append(gear)
        
        # Trim history
        if len(throttle_history) > MAX_HISTORY_POINTS:
            throttle_history.pop(0)
            brake_history.pop(0)
            steering_history.pop(0)
            speed_history.pop(0)
            rpm_history.pop(0)
            gear_history.pop(0)
        
        # Update charts
        if i % CHART_UPDATE_FREQUENCY == 0:
            # Throttle & Brake
            throttle_brake_fig = go.Figure()
            throttle_brake_fig.add_trace(go.Scatter(
                y=throttle_history, 
                mode='lines',
                name='Throttle',
                line=dict(color='green')
            ))
            throttle_brake_fig.add_trace(go.Scatter(
                y=brake_history, 
                mode='lines',
                name='Brake',
                line=dict(color='red')
            ))
            throttle_brake_fig.update_layout(
                height=CHART_HEIGHT_SMALL,
                margin=dict(l=0, r=0, t=30, b=0),
                yaxis=dict(range=THROTTLE_BRAKE_Y_RANGE, title='%'),
                xaxis=dict(showticklabels=False),
                title_text='Throttle & Brake',
                legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
            )
            throttle_brake_chart.plotly_chart(throttle_brake_fig)
            
            # Steering
            steering_fig = go.Figure()
            steering_fig.add_trace(go.Scatter(
                y=steering_history, 
                mode='lines',
                name='Steering'
            ))
            steering_fig.update_layout(
                height=CHART_HEIGHT_SMALL,
                margin=dict(l=0, r=0, t=30, b=0),
                yaxis=dict(range=STEERING_Y_RANGE, title='Angle'),
                xaxis=dict(showticklabels=False),
                title_text='Steering Wheel Angle'
            )
            steering_chart.plotly_chart(steering_fig)
            
            # Speed & RPM with Gear
            speed_rpm_fig = go.Figure()
            speed_rpm_fig.add_trace(go.Scatter(
                y=speed_history,
                mode='lines',
                name='Speed (km/h)',
                line=dict(color='blue')
            ))
            speed_rpm_fig.add_trace(go.Scatter(
                y=rpm_history,
                mode='lines',
                name='RPM',
                line=dict(color='orange'),
                yaxis='y2'
            ))
            
            # Gear changes
            gear_changes = []
            prev_gear = None
            for idx, gear_val in enumerate(gear_history):
                if gear_val != prev_gear:
                    gear_changes.append((idx, gear_val))
                    prev_gear = gear_val
            
            annotations = []
            shapes = []
            for idx, gear_val in gear_changes:
                if idx < len(gear_history) - 1:
                    shapes.append(dict(
                        type="line", x0=idx, y0=0, x1=idx, y1=1,
                        xref="x", yref="paper",
                        line=dict(color="rgba(255, 0, 0, 0.5)", width=2, dash="dash")
                    ))
                    annotations.append(dict(
                        x=idx, y=1.05, xref='x', yref='paper',
                        text=f'G{gear_val}', showarrow=True,
                        arrowhead=2, ax=0, ay=-15,
                        bgcolor="rgba(255, 255, 255, 0.8)",
                        font=dict(size=10)
                    ))
            
            speed_rpm_fig.update_layout(
                height=CHART_HEIGHT_MEDIUM,
                margin=dict(l=0, r=0, t=40, b=0),
                yaxis=dict(title='Speed (km/h)', side='left', range=SPEED_Y_RANGE),
                yaxis2=dict(title='RPM', side='right', overlaying='y', range=RPM_Y_RANGE),
                xaxis=dict(showticklabels=False),
                title_text='Speed & RPM with Gears',
                legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
                annotations=annotations,
                shapes=shapes
            )
            speed_rpm_gear_chart.plotly_chart(speed_rpm_fig)
        
        # Update map
        map_update_counter += 1
        if map_update_counter >= map_update_frequency:
            map_update_counter = 0
            
            current_lat = row.get('telemetry:Lat', 0)
            current_lon = row.get('telemetry:Lon', 0)
            
            # Check for NaN values
            if pd.isna(current_lat) or pd.isna(current_lon):
                current_lat = 0
                current_lon = 0
            
            if current_lat != 0 and current_lon != 0:
                if not map_initialized:
                    initial_view_state.latitude = current_lat
                    initial_view_state.longitude = current_lon
                    map_initialized = True
                
                # Update view state to follow the car (auto-tracking)
                initial_view_state.latitude = current_lat
                initial_view_state.longitude = current_lon
                
                point_data = [{'lat': current_lat, 'lon': current_lon}]
                point_layer.data = point_data
                deck.initial_view_state = initial_view_state
                deck.layers = [point_layer]
                
                # Use the map_container explicitly to avoid overlay issues
                with col2:
                    map_container.pydeck_chart(deck)
        
        time.sleep(row_sleep_time)
    
    # Completion
    st.success(f"Lap replay complete! ({total_rows} data points)")
    time.sleep(RESTART_DELAY_SECONDS)
    st.session_state.load_lap = False
    try:
        st.experimental_rerun()
    except AttributeError:
        st.rerun()


def main():
    fastest_lap_dashboard()


if __name__ == "__main__":
    main()