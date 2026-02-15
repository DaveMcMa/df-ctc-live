import asyncio
import json
import time
import websockets
from confluent_kafka import Producer
import signal
import sys

# ============================================================================
# CONFIGURATION - Edit these values
# ============================================================================
WEBSOCKET_URI = "ws://10.1.241.43:8766"  # WebSocket server URI
STREAM_PATH = '/mapr/ctc-core/ctcf1'      # Kafka stream path
TARGET_TOPIC = 'test'                      # Kafka topic name

# Producer optimization settings
QUEUE_BUFFERING_MAX_MS = 5                 # Very low buffering time (5ms) - flush more frequently
MESSAGE_SEND_MAX_RETRIES = 3               # Retry a few times if sending fails
RETRY_BACKOFF_MS = 10                      # Short backoff between retries
LINGER_MS = 0                              # Don't wait to accumulate messages - send immediately

# Monitoring and flushing settings
STATS_REPORT_INTERVAL = 2.0                # Report stats every N seconds
FLUSH_INTERVAL = 0.2                       # Force flush every N seconds (200ms)
FLUSH_TIMEOUT = 0.1                        # Timeout for periodic flush (100ms)
REGULAR_FLUSH_TIMEOUT = 0.05               # Timeout for regular flushes (50ms)
FLUSH_EVERY_N_MESSAGES = 10                # Flush every N messages

# Connection and timeout settings
WEBSOCKET_RECV_TIMEOUT = 0.5               # Timeout for receiving WebSocket messages
RECONNECT_DELAY = 5                        # Delay before reconnecting after failure (seconds)
MONITOR_CHECK_INTERVAL = 0.05              # Monitor task check interval (50ms)
ERROR_SLEEP_INTERVAL = 0.1                 # Sleep after processing error (100ms)
FINAL_FLUSH_TIMEOUT = 5.0                  # Generous timeout for final flush on shutdown
# ============================================================================

# Initialize the Kafka producer with optimized settings
producer_config = {
    'streams.producer.default.stream': STREAM_PATH,
    'queue.buffering.max.ms': QUEUE_BUFFERING_MAX_MS,
    'message.send.max.retries': MESSAGE_SEND_MAX_RETRIES,
    'retry.backoff.ms': RETRY_BACKOFF_MS,
    'linger.ms': LINGER_MS
}

producer = Producer(producer_config)

# Track message statistics
stats = {
    'messages_sent': 0,
    'last_sent_time': time.time(),
    'start_time': time.time(),
    'flush_count': 0,
    'last_flush_time': time.time(),
    'flush_duration_total': 0
}

# For periodic status reporting
last_stats_time = time.time()

# Handle graceful shutdown
running = True

def delivery_callback(err, msg):
    """Callback executed when message is successfully delivered or fails"""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        # Message delivered successfully
        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        timestamp = msg.timestamp()[1]
        
        # Uncomment for debugging individual message delivery
        # print(f"Message delivered to {topic} [{partition}] at offset {offset} with timestamp {timestamp}")
        pass

async def monitor_producer():
    """Periodically monitor and report producer status"""
    global last_stats_time
    
    while running:
        current_time = time.time()
        if current_time - last_stats_time >= STATS_REPORT_INTERVAL:
            # Calculate message rate
            elapsed = current_time - stats['start_time']
            rate = stats['messages_sent'] / elapsed if elapsed > 0 else 0
            
            # Calculate time since last message
            time_since_last = current_time - stats['last_sent_time']
            
            # Calculate average flush time
            avg_flush_time = (stats['flush_duration_total'] / stats['flush_count']) * 1000 if stats['flush_count'] > 0 else 0
            
            print(f"Producer stats: {stats['messages_sent']} msgs sent ({rate:.1f} msgs/sec), "
                  f"Last msg: {time_since_last:.2f}s ago, "
                  f"Flush count: {stats['flush_count']}, "
                  f"Avg flush time: {avg_flush_time:.2f}ms")
            
            last_stats_time = current_time
        
        # Force a flush every FLUSH_INTERVAL to ensure messages are sent
        if current_time - stats['last_flush_time'] >= FLUSH_INTERVAL:
            flush_start = time.time()
            remaining = producer.flush(timeout=FLUSH_TIMEOUT)
            flush_duration = time.time() - flush_start
            
            stats['flush_count'] += 1
            stats['flush_duration_total'] += flush_duration
            stats['last_flush_time'] = current_time
            
            if remaining > 0:
                print(f"WARNING: {remaining} messages still in queue after flush")
        
        await asyncio.sleep(MONITOR_CHECK_INTERVAL)

async def listen_for_speed():
    """Connect to WebSocket and send messages to Kafka"""
    
    # Keep trying to connect if connection fails
    while running:
        try:
            async with websockets.connect(WEBSOCKET_URI) as websocket:
                print(f"Connected to WebSocket server at {WEBSOCKET_URI}")
                
                while running:
                    try:
                        # Wait for incoming message with timeout
                        speed = await asyncio.wait_for(websocket.recv(), timeout=WEBSOCKET_RECV_TIMEOUT)
                        
                        # Add timestamp to message
                        try:
                            # Try to parse as JSON if it's already JSON
                            data = json.loads(speed)
                            if isinstance(data, dict) and 'timestamp' not in data:
                                data['timestamp'] = time.time()
                            enriched_message = json.dumps(data)
                        except json.JSONDecodeError:
                            # Not JSON, create a simple JSON with the raw value and timestamp
                            enriched_message = json.dumps({
                                'value': speed,
                                'timestamp': time.time()
                            })
                        
                        # Send the message to Kafka
                        producer.produce(
                            TARGET_TOPIC, 
                            value=enriched_message,
                            callback=delivery_callback
                        )
                        
                        # Update stats
                        stats['messages_sent'] += 1
                        stats['last_sent_time'] = time.time()
                        
                        # Poll the producer to handle callbacks and network I/O
                        producer.poll(0)
                        
                        # Flush every N messages to ensure timely delivery
                        if stats['messages_sent'] % FLUSH_EVERY_N_MESSAGES == 0:
                            flush_start = time.time()
                            producer.flush(timeout=REGULAR_FLUSH_TIMEOUT)
                            stats['flush_count'] += 1
                            stats['flush_duration_total'] += time.time() - flush_start
                            stats['last_flush_time'] = time.time()
                        
                    except asyncio.TimeoutError:
                        # No message received within timeout, just continue
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        print("Connection closed by server, reconnecting...")
                        break
                    except Exception as e:
                        print(f"Error processing message: {e}")
                        # Continue running despite errors
                        await asyncio.sleep(ERROR_SLEEP_INTERVAL)
        
        except (websockets.exceptions.InvalidStatusCode, 
                websockets.exceptions.InvalidURI,
                ConnectionRefusedError) as connection_error:
            print(f"Failed to connect to WebSocket server: {connection_error}")
            print(f"Retrying in {RECONNECT_DELAY} seconds...")
            await asyncio.sleep(RECONNECT_DELAY)
        except Exception as e:
            print(f"Unexpected error: {e}")
            print(f"Retrying in {RECONNECT_DELAY} seconds...")
            await asyncio.sleep(RECONNECT_DELAY)

def handle_signal(sig, frame):
    """Handle interrupt signals to gracefully shutdown"""
    global running
    print("\nShutdown signal received. Cleaning up...")
    running = False

if __name__ == "__main__":
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    # Create and run the event loop
    loop = asyncio.get_event_loop()
    
    try:
        # Start both the main listener and the monitor task
        monitor_task = loop.create_task(monitor_producer())
        listener_task = loop.create_task(listen_for_speed())
        
        # Wait for both tasks to complete
        loop.run_until_complete(asyncio.gather(listener_task, monitor_task))
    except KeyboardInterrupt:
        print("Script interrupted by user.")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        # Ensure proper cleanup
        print("Shutting down producer...")
        remaining = producer.flush(timeout=FINAL_FLUSH_TIMEOUT)
        if remaining > 0:
            print(f"Warning: {remaining} messages may have been lost")
        
        # Clean up tasks
        for task in asyncio.all_tasks(loop):
            task.cancel()
        
        # Close the loop
        loop.close()
        print("Shutdown complete.")