import streamlit as st
import json
import time
from confluent_kafka import Consumer, KafkaError
import threading
import queue
from collections import deque

# ============================================================================
# CONFIGURATION - Edit these values
# ============================================================================
DEFAULT_STREAM_PATH = "/mapr/ctc-core/ctcf1"
DEFAULT_TOPIC_NAME = "test"
DEFAULT_POLLING_FREQUENCY = 60  # Hz
DEFAULT_BUFFER_SIZE = 30
DEFAULT_RESET_OFFSET = False
DEFAULT_SHOW_DEBUG = False
# ============================================================================

# Streamlit UI setup
st.set_page_config(page_title="HPE Racing Telemetry", layout="wide")
st.title("HPE Data Fabric Real-Time Race Telemetry")

# Function to fetch the first message from Kafka (for metadata)
def fetch_first_message(stream_path, target_topic):
    try:
        # Create a consumer with a unique group ID to ensure we start fresh
        consumer_config = {
            'streams.consumer.default.stream': stream_path,
            'group.id': f'metadata-fresh-group-{int(time.time())}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'default.topic.config': {'auto.offset.reset': 'earliest'}
        }
        
        consumer = Consumer(consumer_config)
        
        # Subscribe to the topic with earliest offset
        consumer.subscribe([target_topic])
        
        # Poll for messages with a timeout
        for attempt in range(3):  # Try up to 3 times
            msg = consumer.poll(timeout=5.0)  # 5 second timeout
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == -191:  # PARTITION_EOF
                    continue
                else:
                    consumer.close()
                    return None
            
            # If we get here, we have a message
            message_value = msg.value()
            try:
                # Process the message
                data = json.loads(message_value)
                consumer.close()
                return data
            except json.JSONDecodeError:
                consumer.close()
                return None
        
        # If we get here, we failed to get a message after multiple attempts
        consumer.close()
        return None
        
    except Exception as e:
        return None

# Sidebar with metadata
with st.sidebar:
    # Add logo at the top
    try:
        st.image("logo.png")
    except:
        st.warning("Logo image not found")
    
    # Fetch metadata for sidebar
    # Create variables for stream_path and target_topic that will be used later
    stream_path = DEFAULT_STREAM_PATH
    target_topic = DEFAULT_TOPIC_NAME
    
    # Fetch metadata on sidebar load
    metadata = fetch_first_message(stream_path, target_topic)
    
    if metadata:
        # Session info
        session_id = metadata.get('UniqueSessionID', 'N/A')
        session_state = metadata.get('SessionState', 'N/A')
        st.sidebar.subheader("Session Details")
        st.sidebar.text(f"Session ID: {session_id}")
        st.sidebar.text(f"Session State: {session_state}")
        
        # Driver info
        driver_info = metadata.get("DriverInfo", {})
        driver_info_drivers = driver_info.get("Drivers", [])
        
        if driver_info_drivers and len(driver_info_drivers) > 0:
            st.sidebar.subheader("Driver Information")
            driver = driver_info_drivers[0]
            st.sidebar.text(f"Driver Name: {driver.get('UserName', 'N/A')}")
            st.sidebar.text(f"Car: {driver.get('CarScreenName', 'N/A')}")
        
        # Race info
        race_info = metadata.get("RaceInfo", {})
        if race_info:
            st.sidebar.subheader("Track Information")
            st.sidebar.text(f"Track: {race_info.get('TrackDisplayName', 'N/A')}")
            st.sidebar.text(f"Type: {race_info.get('TrackType', 'N/A')}")
            st.sidebar.text(f"Track Temp: {race_info.get('TrackSurfaceTemp', 'N/A')}")
            
            # Weather info
            weekend_options = race_info.get("WeekendOptions", {})
            if weekend_options:
                st.sidebar.subheader("Weather")
                st.sidebar.text(f"Weather: {weekend_options.get('Skies', 'N/A')}")
                st.sidebar.text(f"Temperature: {weekend_options.get('WeatherTemp', 'N/A')}")
                st.sidebar.text(f"Wind: {weekend_options.get('WindSpeed', 'N/A')}")
                st.sidebar.text(f"Humidity: {weekend_options.get('RelativeHumidity', 'N/A')}")
        
        # Car info - tire and chassis data
        car_info = metadata.get("CarInfo", {})
        if car_info:
            st.sidebar.subheader("Tire Information")
            
            # Extract tire data
            tires_info = car_info.get("Tires", {})
            chassis_info = car_info.get("Chassis", {})
            
            # Left Front
            lf_tires = tires_info.get("LeftFront", {})
            st.sidebar.text("Left Front Tire:")
            st.sidebar.text(f"  Pressure: {lf_tires.get('StartingPressure', 'N/A')}")
            st.sidebar.text(f"  Temp: {lf_tires.get('LastTempsOMI', 'N/A')}")
            st.sidebar.text(f"  Tread: {lf_tires.get('TreadRemaining', 'N/A')}")
            
            # Right Front
            rf_tires = tires_info.get("RightFront", {})
            st.sidebar.text("Right Front Tire:")
            st.sidebar.text(f"  Pressure: {rf_tires.get('StartingPressure', 'N/A')}")
            st.sidebar.text(f"  Temp: {rf_tires.get('LastTempsOMI', 'N/A')}")
            st.sidebar.text(f"  Tread: {rf_tires.get('TreadRemaining', 'N/A')}")
            
            # Left Rear
            lr_tires = tires_info.get("LeftRear", {})
            st.sidebar.text("Left Rear Tire:")
            st.sidebar.text(f"  Pressure: {lr_tires.get('StartingPressure', 'N/A')}")
            st.sidebar.text(f"  Temp: {lr_tires.get('LastTempsOMI', 'N/A')}")
            st.sidebar.text(f"  Tread: {lr_tires.get('TreadRemaining', 'N/A')}")
            
            # Right Rear
            rr_tires = tires_info.get("RightRear", {})
            st.sidebar.text("Right Rear Tire:")
            st.sidebar.text(f"  Pressure: {rr_tires.get('StartingPressure', 'N/A')}")
            st.sidebar.text(f"  Temp: {rr_tires.get('LastTempsOMI', 'N/A')}")
            st.sidebar.text(f"  Tread: {rr_tires.get('TreadRemaining', 'N/A')}")
    else:
        st.sidebar.warning("No metadata available")
        
    # Add configuration section at the very bottom of sidebar
    st.sidebar.markdown("---")
    st.sidebar.header("Configuration")
    
    polling_frequency = st.sidebar.slider("Kafka Polling Frequency (Hz)", min_value=10, max_value=200, value=DEFAULT_POLLING_FREQUENCY)
    buffer_size = st.sidebar.slider("Message Buffer Size", min_value=5, max_value=100, value=DEFAULT_BUFFER_SIZE)
    reset_offset = st.sidebar.checkbox("Reset to Earliest Offset", value=DEFAULT_RESET_OFFSET)
    show_debug = st.sidebar.checkbox("Show Debug Log", value=DEFAULT_SHOW_DEBUG)
    
    st.sidebar.markdown("---")
    # Stream path and topic name at the very bottom
    stream_path = st.sidebar.text_input("Stream Path", value=stream_path)
    target_topic = st.sidebar.text_input("Topic Name", value=target_topic)

# Kafka consumer configuration
consumer_config = {
    'streams.consumer.default.stream': stream_path,
    'group.id': 'telemetry-group',
    'auto.offset.reset': 'earliest' if reset_offset else 'latest',
    'enable.auto.commit': True,
    'auto.commit.interval.ms': 100,
    'session.timeout.ms': 6000,
    'fetch.wait.max.ms': 10,
    'fetch.error.backoff.ms': 5,
    'fetch.min.bytes': 1
}

# Create communication channels
data_queue = queue.Queue(maxsize=buffer_size)
debug_queue = queue.Queue(maxsize=50)
stop_event = threading.Event()
debug_log = deque(maxlen=25)

# Function to add debug messages
def add_debug(message):
    if debug_queue.qsize() < 40:
        timestamp = time.strftime("%H:%M:%S.%f")[:-3]
        try:
            debug_queue.put_nowait(f"[{timestamp}] {message}")
        except queue.Full:
            pass

# Background consumer thread function
def kafka_consumer_thread():
    try:
        add_debug("Starting consumer thread")
        consumer = Consumer(consumer_config)
        
        try:
            add_debug(f"Subscribing to topic: {target_topic}")
            consumer.subscribe([target_topic])
            
            message_count = 0
            consecutive_empty_polls = 0
            
            while not stop_event.is_set():
                try:
                    # Poll with short timeout for responsiveness
                    msg = consumer.poll(timeout=0.01)
                    
                    if msg is None:
                        consecutive_empty_polls += 1
                        if consecutive_empty_polls > 100:
                            time.sleep(0.01)
                        continue
                    else:
                        consecutive_empty_polls = 0
                    
                    # Check for errors
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            add_debug(f"Kafka error: {msg.error().str()}")
                        continue
                    
                    # Process message
                    message_count += 1
                    
                    try:
                        value = msg.value()
                        if value is None:
                            continue
                            
                        data = json.loads(value.decode('utf-8'))
                        
                        # Add to queue, replacing oldest if full
                        try:
                            data_queue.put_nowait(data)
                        except queue.Full:
                            try:
                                data_queue.get_nowait()  # Remove oldest
                                data_queue.put_nowait(data)
                            except:
                                pass
                        
                    except Exception as e:
                        if not isinstance(e, json.JSONDecodeError):
                            add_debug(f"Message processing error: {str(e)}")
                
                except Exception as e:
                    add_debug(f"Polling error: {str(e)}")
                    time.sleep(0.05)
        
        except Exception as e:
            add_debug(f"Subscription error: {str(e)}")
        
        finally:
            consumer.close()
            
    except Exception as e:
        add_debug(f"Consumer initialization error: {str(e)}")

# Start the consumer thread
consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
consumer_thread.start()

# Format time in MM:SS.ms format
def format_time(seconds):
    if not seconds or seconds == 0:
        return "00:00.000"
    minutes = int(seconds // 60)
    seconds_part = seconds % 60
    return f"{minutes:02d}:{int(seconds_part):02d}.{int((seconds_part % 1) * 1000):03d}"

# Create the main layout
st.header("Car Telemetry")

# Top row - Speed, RPM, Gear
speed_rpm_gear = st.columns([2, 2, 1])

# Speed display with progress bar
speed_display = speed_rpm_gear[0].container()
speed_display.markdown("**Speed**")
speed_value_display = speed_display.empty()
speed_bar = speed_display.empty()

# RPM display with progress bar
rpm_display = speed_rpm_gear[1].container()
rpm_display.markdown("**RPM**")
rpm_value_display = rpm_display.empty()
rpm_bar = rpm_display.empty()

# Gear display
gear_display = speed_rpm_gear[2].empty()

# Pedals - Only Throttle and Brake (removed clutch)
st.subheader("Pedals")
pedal_labels = st.columns(2)
pedal_labels[0].markdown("**Throttle**")
pedal_labels[1].markdown("**Brake**")

pedal_bars = st.columns(2)
throttle_container = pedal_bars[0].container()
throttle_bar = throttle_container.empty()
throttle_value = throttle_container.empty()

brake_container = pedal_bars[1].container()
brake_bar = brake_container.empty()
brake_value = brake_container.empty()

# Steering wheel - improved visualization
steering_container = st.container()
steering_value = steering_container.empty()
steering_bar = steering_container.empty()

# Lap times
st.header("Lap Information")
lap_cols = st.columns(3)
lap_current = lap_cols[0].empty()
lap_best = lap_cols[1].empty()
lap_last = lap_cols[2].empty()

# Session information
st.header("Session Information")
session_cols = st.columns(4)
session_id_display = session_cols[0].empty()
session_state_display = session_cols[1].empty()
session_time_display = session_cols[2].empty()
session_tick_display = session_cols[3].empty()

# Performance metrics
st.header("Telemetry Performance")
perf_cols = st.columns(3)
message_rate_display = perf_cols[0].empty()
latency_display = perf_cols[1].empty()
consumer_status_display = perf_cols[2].empty()

# Debug display
if show_debug:
    st.header("Debug Information")
    debug_display = st.empty()

# Stats for tracking
message_history = deque(maxlen=100)
latency_values = deque(maxlen=50)
last_update_time = time.time()

# Main UI loop
try:
    # Initialize tracking variables
    last_timestamp = time.time()
    
    # Define max values for progress bars
    max_speed = 350  # km/h
    max_rpm = 15000  # rpm
    
    while True:
        # Process debug messages if requested
        if show_debug and debug_queue.qsize() > 0:
            for _ in range(min(5, debug_queue.qsize())):
                try:
                    debug_msg = debug_queue.get_nowait()
                    debug_log.append(debug_msg)
                except queue.Empty:
                    break
            debug_display.code("\n".join(debug_log))
        
        # Get latest telemetry data if available
        updated = False
        try:
            data = data_queue.get_nowait()
            
            # Track message receipt time for rate calculation
            current_time = time.time()
            message_history.append(current_time)
            
            # Calculate latency if timestamp exists
            if isinstance(data, dict) and "timestamp" in data:
                try:
                    sender_timestamp = float(data["timestamp"])
                    latency_ms = (current_time - sender_timestamp) * 1000
                    latency_values.append(latency_ms)
                except:
                    pass
            
            # Update UI with new data
            
            # 1. Speed display
            speed_value = data.get("Speed", 0)
            
            # Display the numeric value
            speed_value_display.markdown(f"### {speed_value:.1f} km/h")
            
            # Update progress bar - normalize to 0-1 range
            speed_normalized = min(1.0, max(0.0, speed_value / max_speed))
            speed_bar.progress(speed_normalized)
            
            # 2. RPM display
            rpm_value = data.get("RPM", 0)
            
            # Display the numeric value
            rpm_value_display.markdown(f"### {rpm_value:.0f} RPM")
            
            # Update progress bar - normalize to 0-1 range
            rpm_normalized = min(1.0, max(0.0, rpm_value / max_rpm))
            rpm_bar.progress(rpm_normalized)
            
            # 3. Gear display
            gear_value = data.get("Gear", 1)
            gear_display.metric(
                "Gear",
                gear_value,
                delta=None
            )
            
            # 4. Pedals (Throttle and Brake only)
            # Get raw values and ensure they're within bounds (0 to 1)
            raw_throttle = data.get("Throttle", 0)
            raw_brake = data.get("Brake", 0)
            
            # Debug throttle and brake raw values
            if show_debug:
                add_debug(f"Raw Throttle: {raw_throttle}, Raw Brake: {raw_brake}")
            
            # Handle special cases where values might be out of expected range
            # If values are very small (like 0.01), we want to ensure they're still visible
            throttle = float(raw_throttle)
            brake = float(raw_brake)
            
            # Ensure values are between 0 and 1 for progress bars
            throttle = max(0.0, min(1.0, throttle))
            brake = max(0.0, min(1.0, brake))
            
            # Update progress bars with normalized values
            throttle_bar.progress(throttle)
            brake_bar.progress(brake)
            
            # Display percentage values
            throttle_value.markdown(f"**{throttle * 100:.0f}%**")
            brake_value.markdown(f"**{brake * 100:.0f}%**")
            
            # 5. Steering wheel visualization (with direction inverted)
            # Get the raw steering angle
            raw_steering_angle = data.get("SteeringWheelAngle", 0)
            
            # Invert the steering angle to correct the direction
            raw_steering_angle = -raw_steering_angle
            
            # Normalize the steering angle to a -1 to 1 range if needed
            # Assuming the angle is already in a reasonable range
            max_angle = 1.0  # Adjust based on your actual data
            normalized_angle = raw_steering_angle / max_angle
            
            # Create a central bar visualization
            # Map from -1...1 to 0...1 for the progress bar
            # The center point (straight) should be 0.5
            bar_value = 0.5 + (normalized_angle * 0.5)
            bar_value = max(0.0, min(1.0, bar_value))  # Clamp to 0-1 range
            
            # Display a label showing direction
            if normalized_angle < -0.05:
                direction = "◀️ LEFT"
            elif normalized_angle > 0.05:
                direction = "RIGHT ▶️"
            else:
                direction = "CENTER"
                
            steering_value.markdown(f"**Angle: {raw_steering_angle:.2f} rad ({direction})**")
            
            # Use an HTML progress bar for more styling control
            html_bar = f"""
            <div style="width:100%; height:30px; background-color:#eee; border-radius:5px; position:relative;">
                <div style="position:absolute; top:0; bottom:0; left:0; width:100%; display:flex;">
                    <div style="flex:1; border-right:2px solid #777;"></div>
                    <div style="flex:1;"></div>
                </div>
                <div style="position:absolute; top:0; bottom:0; left:{bar_value*100}%; width:8px; 
                     background-color:red; transform:translateX(-50%);"></div>
            </div>
            """
            steering_bar.markdown(html_bar, unsafe_allow_html=True)
            
            # 7. Lap times
            current_lap = data.get("Lap", 1)
            current_lap_time = data.get("LapCurrentLapTime", 0)
            best_lap_time = data.get("LapBestLapTime", 0)
            last_lap_time = data.get("LapLastLapTime", 0)
            
            lap_current.metric(
                f"Current Lap ({current_lap})",
                format_time(current_lap_time),
                delta=None
            )
            
            lap_best.metric(
                "Best Lap",
                format_time(best_lap_time) if best_lap_time > 0 else "--:--:---",
                delta=None
            )
            
            lap_last.metric(
                "Last Lap",
                format_time(last_lap_time) if last_lap_time > 0 else "--:--:---",
                delta=None
            )
            
            # 8. Session information
            session_id = data.get("UniqueSessionID", "--")
            session_state = data.get("SessionState", "--")
            session_time = data.get("SessionTime", 0)
            session_tick = data.get("SessionTick", 0)
            
            session_id_display.metric("Session ID", session_id, delta=None)
            session_state_display.metric("State", session_state, delta=None)
            session_time_display.metric("Session Time", format_time(session_time), delta=None)
            session_tick_display.metric("Tick", session_tick, delta=None)
            
            updated = True
            
        except queue.Empty:
            # No new data, just continue
            pass
        
        # Update metrics periodically
        if time.time() - last_update_time >= 2.0 or updated:  # Update every half second or when new data arrives
            # Calculate message rate
            if len(message_history) >= 2:
                time_span = message_history[-1] - message_history[0]
                if time_span > 0:
                    rate = len(message_history) / time_span
                    message_rate_display.metric("Messages/sec", f"{rate:.1f}", delta=None)
            
            # Calculate average latency
            if latency_values:
                avg_latency = sum(latency_values) / len(latency_values)
                max_latency = max(latency_values)
                latency_display.metric("Avg Latency", f"{avg_latency:.1f} ms", delta=None)
            
            # Update consumer status
            time_since_last = time.time() - last_timestamp
            status = "Active" if time_since_last < 1.0 else f"Last update: {time_since_last:.1f}s ago"
            consumer_status_display.metric("Consumer Status", status, delta=None)
            
            if updated:
                last_timestamp = time.time()
            
            last_update_time = time.time()
        
        # Short sleep to prevent CPU spinning
        time.sleep(0.01)
        
except KeyboardInterrupt:
    st.warning("Stopping telemetry...")
finally:
    stop_event.set()
    consumer_thread.join(timeout=1.0)
    st.info("Telemetry stopped. Refresh to restart.")