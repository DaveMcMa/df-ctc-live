import asyncio
import websockets
import irsdk
import json
import datetime
import time

# ============================================================================
# CONFIGURATION - Edit these values
# ============================================================================
# WebSocket Server Configuration
SERVER_HOST = '0.0.0.0'
SERVER_PORT = 8766

# Telemetry Update Configuration
TELEMETRY_UPDATE_RATE = 60  # Target FPS for telemetry updates
NO_CLIENTS_POLL_RATE = 2  # Hz when no clients connected
NOT_CONNECTED_POLL_RATE = 2  # Hz when iRacing not connected

# WebSocket Connection Settings
PING_INTERVAL = 10  # Seconds between pings
PING_TIMEOUT = 5  # Seconds to wait for ping response
CLOSE_TIMEOUT = 2  # Seconds to wait for connection close

# Session Detection
SESSION_DETECTION_INTERVAL = 1  # Check for session changes every N seconds

# Retry Configuration
MAX_SERVER_RETRIES = 5
RETRY_DELAY = 5  # Seconds to wait before retry

# Speed Conversion
SPEED_CONVERSION_FACTOR = 3.6  # Convert m/s to km/h
# ============================================================================

print("Starting iRacing Telemetry WebSocket Server...")

# Global variables
ir = irsdk.IRSDK()
connected_clients = set()
current_session_id = None
last_connection_state = False

def generate_unique_session_id():
    """Generate a unique session ID based on current timestamp"""
    return datetime.datetime.now().strftime("%d%m%Y%H%M%S")

def check_iracing_connection():
    """Check if iRacing is running and connected"""
    if ir.is_initialized and ir.is_connected:
        return True
    
    # Try to connect if not already connected
    try:
        ir.startup()
        return ir.is_initialized and ir.is_connected
    except Exception as e:
        print(f"Error connecting to iRacing: {e}")
        return False

def get_telemetry_data():
    """Retrieve the required telemetry data from iRacing SDK."""
    if not check_iracing_connection():
        return None
    
    try:
        return {
            'metadata': {
                'UniqueSessionID': current_session_id,
                'RaceInfo': ir['WeekendInfo'],
                'SessionInfo': ir['SessionInfo'],
                'CarInfo': ir['CarSetup'],
                'DriverInfo': ir['DriverInfo'],
                'timestamp': datetime.datetime.now().isoformat()
            },
            'telemetry': {
                'UniqueSessionID': current_session_id,
                'SessionTime': ir['SessionTime'],
                'SessionTick': ir['SessionTick'],
                'SessionNum': ir['SessionNum'],
                'SessionState': ir['SessionState'],
                'SessionUniqueID': ir['SessionUniqueID'],
                'Speed': SPEED_CONVERSION_FACTOR * ir['Speed'],
                'Yaw': ir['Yaw'],
                'LapBestLap': ir['LapBestLap'],
                'LapBestLapTime': ir['LapBestLapTime'],
                'LapLastLapTime': ir['LapLastLapTime'],
                'LapCurrentLapTime': ir['LapCurrentLapTime'],
                'SteeringWheelAngle': ir['SteeringWheelAngle'],
                'Throttle': ir['Throttle'],
                'Brake': ir['Brake'],
                'Clutch': ir['Clutch'],
                'Gear': ir['Gear'],
                'RPM': ir['RPM'],
                'Lap': ir['Lap'],
                'LapCompleted': ir['LapCompleted'],
                'timestamp': datetime.datetime.now().isoformat()
            }
        }
    except Exception as e:
        print(f"Error getting telemetry data: {e}")
        return None

async def detect_session_change():
    """Detect if a new session has started based on iRacing's session ID"""
    global current_session_id, last_connection_state
    
    while True:
        iracing_connected = check_iracing_connection()
        
        # Detect connection state changes
        if iracing_connected != last_connection_state:
            if iracing_connected:
                print("iRacing connected!")
                # Generate a new session ID when iRacing connects
                current_session_id = generate_unique_session_id()
                print(f"New session started with ID: {current_session_id}")
                
                # Broadcast the new session to all clients
                if connected_clients:
                    metadata = get_telemetry_data()
                    if metadata:
                        await broadcast_message(json.dumps({
                            'event': 'session_start',
                            'metadata': metadata['metadata']
                        }))
            else:
                print("iRacing disconnected!")
                # Notify clients about disconnection
                if connected_clients and current_session_id:
                    await broadcast_message(json.dumps({
                        'event': 'session_end',
                        'sessionId': current_session_id
                    }))
            
            last_connection_state = iracing_connected
        
        # If connected, check for session ID changes
        if iracing_connected and ir['SessionUniqueID']:
            session_unique_id = ir['SessionUniqueID']
            
            # Check if we need a new unique ID (session change within iRacing)
            if ir['SessionNum'] == 0 and ir['SessionTime'] < 1:  # Likely a new session started
                new_session_id = generate_unique_session_id()
                
                if new_session_id != current_session_id:
                    print(f"Session changed: {current_session_id} -> {new_session_id}")
                    current_session_id = new_session_id
                    
                    # Broadcast the new session to all clients
                    if connected_clients:
                        metadata = get_telemetry_data()
                        if metadata:
                            await broadcast_message(json.dumps({
                                'event': 'session_change',
                                'metadata': metadata['metadata']
                            }))
        
        await asyncio.sleep(SESSION_DETECTION_INTERVAL)

async def broadcast_message(message):
    """Send a message to all connected clients"""
    if not connected_clients:
        return
    
    # Create a copy of the set to avoid "Set changed size during iteration" error
    clients_to_process = connected_clients.copy()
    
    disconnected_clients = set()
    for client in clients_to_process:
        try:
            await client.send(message)
        except websockets.exceptions.ConnectionClosed:
            disconnected_clients.add(client)
        except Exception as e:
            print(f"Error sending message to client: {e}")
            disconnected_clients.add(client)
    
    # Remove disconnected clients
    for client in disconnected_clients:
        if client in connected_clients:  # Check if client is still in the set
            connected_clients.remove(client)
    
    if disconnected_clients:
        print(f"Removed {len(disconnected_clients)} disconnected client(s). {len(connected_clients)} remaining.")

async def send_telemetry_data():
    """Send telemetry data to all connected clients"""
    prev_session_time = None
    
    while True:
        try:
            if not connected_clients:
                await asyncio.sleep(1/NO_CLIENTS_POLL_RATE)
                continue
                
            if not check_iracing_connection():
                await asyncio.sleep(1/NOT_CONNECTED_POLL_RATE)
                continue
            
            data = get_telemetry_data()
            if data and 'telemetry' in data:
                telemetry = data['telemetry']
                
                # Only log if session time has changed (to reduce console spam)
                if telemetry['SessionTime'] != prev_session_time:
                    print(f"Session Time: {telemetry['SessionTime']:.2f} | Clients: {len(connected_clients)}")
                    prev_session_time = telemetry['SessionTime']
                
                # Send telemetry to all clients
                await broadcast_message(json.dumps(telemetry))
            
            await asyncio.sleep(1/TELEMETRY_UPDATE_RATE)
            
        except Exception as e:
            print(f"Error in telemetry sender: {e}")
            await asyncio.sleep(1)  # Wait a bit before retrying on error

async def client_handler(websocket):
    """Handle a client WebSocket connection"""
    try:
        client_info = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
        print(f"New client connected: {client_info}")
        
        # Add to connected clients with safeguard
        if websocket not in connected_clients:
            connected_clients.add(websocket)
            print(f"Total connected clients: {len(connected_clients)}")
        
        # Send initial metadata if we're connected to iRacing
        if check_iracing_connection() and current_session_id:
            data = get_telemetry_data()
            if data and 'metadata' in data:
                try:
                    await websocket.send(json.dumps({
                        'event': 'connection_established',
                        'metadata': data['metadata']
                    }))
                except Exception as e:
                    print(f"Error sending initial data to client {client_info}: {e}")
                    if websocket in connected_clients:
                        connected_clients.remove(websocket)
                    return
        
        try:
            # Keep the connection open and handle any messages from the client
            async for message in websocket:
                # Process heartbeats or other client messages if needed
                try:
                    msg_data = json.loads(message)
                    if 'type' in msg_data and msg_data['type'] == 'heartbeat':
                        # Respond to heartbeat if client implements it
                        await websocket.send(json.dumps({'type': 'heartbeat_ack'}))
                except json.JSONDecodeError:
                    # Not JSON or not a heartbeat, ignore
                    pass
        except websockets.exceptions.ConnectionClosed as e:
            print(f"Client disconnected: {client_info} - Code: {e.code}, Reason: {e.reason}")
        except Exception as e:
            print(f"Error handling messages from client {client_info}: {e}")
    except Exception as e:
        print(f"Unexpected error in client handler: {e}")
    finally:
        # Always ensure client is removed from the set when connection ends
        if websocket in connected_clients:
            connected_clients.remove(websocket)
            print(f"Client removed: {client_info}. Total clients: {len(connected_clients)}")

async def main():
    """Start the WebSocket server and background tasks"""
    # Initialize the session ID
    global current_session_id
    current_session_id = generate_unique_session_id()
    
    # Create tasks with proper error handling
    session_detector_task = asyncio.create_task(detect_session_change())
    session_detector_task.add_done_callback(
        lambda t: print(f"Session detector task ended: {t.exception() if t.exception() else 'No exception'}")
    )
    
    telemetry_sender_task = asyncio.create_task(send_telemetry_data())
    telemetry_sender_task.add_done_callback(
        lambda t: print(f"Telemetry sender task ended: {t.exception() if t.exception() else 'No exception'}")
    )
    
    # Start the WebSocket server
    print(f"Starting WebSocket server on ws://{SERVER_HOST}:{SERVER_PORT}")
    
    # Keep track of the server for proper shutdown
    server = None
    
    try:
        server = await websockets.serve(
            client_handler, 
            SERVER_HOST, 
            SERVER_PORT, 
            ping_interval=PING_INTERVAL, 
            ping_timeout=PING_TIMEOUT,
            close_timeout=CLOSE_TIMEOUT
        )
        
        print(f"WebSocket server is running on port {SERVER_PORT}!")
        
        # Keep the server running indefinitely
        await asyncio.Future()
    finally:
        # Proper cleanup
        if server:
            server.close()
            await server.wait_closed()
            print("WebSocket server closed")
        
        # Cancel background tasks
        session_detector_task.cancel()
        telemetry_sender_task.cancel()
        
        # Wait for tasks to finish
        try:
            await asyncio.gather(session_detector_task, telemetry_sender_task, return_exceptions=True)
        except asyncio.CancelledError:
            pass

async def start_with_retry(max_retries=MAX_SERVER_RETRIES, retry_delay=RETRY_DELAY):
    """Start the server with retry logic"""
    retries = 0
    while retries < max_retries:
        try:
            await main()
            break  # If main completes normally (it shouldn't unless there's an error)
        except OSError as e:
            if e.errno == 10048:  # Address already in use
                print(f"Port already in use. Waiting {retry_delay} seconds to retry...")
                retries += 1
                await asyncio.sleep(retry_delay)
            else:
                print(f"OS error: {e}")
                break
        except Exception as e:
            print(f"Server error: {e}")
            if retries < max_retries - 1:
                print(f"Restarting server in {retry_delay} seconds... (Attempt {retries + 1}/{max_retries})")
                retries += 1
                await asyncio.sleep(retry_delay)
            else:
                print(f"Maximum retry attempts ({max_retries}) reached. Shutting down.")
                break

if __name__ == "__main__":
    try:
        # Use the retry wrapper
        asyncio.run(start_with_retry())
    except KeyboardInterrupt:
        print("Server shutting down by user request")
    except Exception as e:
        print(f"Fatal server error: {e}")
    finally:
        # Clean up
        if ir.is_initialized:
            ir.shutdown()
        print("Server stopped")