import paho.mqtt.client as mqtt
import time
import sys

# --- Configuration ---
BROKER_ADDRESS = "localhost"
BROKER_PORT = 1883
TOPIC = "test/topic/temperature"
MESSAGE = "Hello, MQTT from Python!"
COUNT=100
# ---------------------

def on_connect(client, userdata, flags, rc):
    """
    The callback for when the client receives a CONNACK response from the server.
    rc (return code) 0 means success.
    """
    if rc == 0:
        print("Connected successfully to MQTT broker.")
        # Once connected, publish the message
        publish_message(client)
    else:
        print(f"Connection failed with code {rc}. Check if the broker is running.")
        sys.exit(1)

def on_publish(client, userdata, mid):
    """
    The callback for when a message has been published successfully.
    mid is the message ID of the published message.
    """
    if not hasattr(client, '_message_count'):
        client._message_count = 0
    client._message_count += 1
    print(f"Message {client._message_count} published (mid: {mid}).")
    
    # Disconnect after publishing COUNT messages
    if client._message_count >= COUNT:
        print("All messages published. Disconnecting...")
        client.disconnect()
        # A small sleep to let the disconnect complete before the script ends
        time.sleep(0.5)

def publish_message(client):
    """Publishes N messages to the configured topic."""
    print(f"Attempting to publish {COUNT} messages to topic '{TOPIC}'...")
    # Initialize message counter
    client._message_count = 0
    for i in range(COUNT):
        message = f"{MESSAGE} (#{i+1})"
        # The publish call returns a result object. We use QoS 1.
        # QoS 1 means the message is delivered at least once.
        client.publish(TOPIC, message, qos=1)
        print(f"Wrote {i} message")
        time.sleep(0.1)  # Small delay between messages to avoid flooding

def main():
    """Main function to set up and run the MQTT client."""
    print(f"Starting MQTT client. Target: {BROKER_ADDRESS}:{BROKER_PORT}")

    # Create a new MQTT client instance (using API version 1 for broader compatibility)
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "demoClient")

    client.username_pw_set("demo", "demo")

    # Assign callback functions
    client.on_connect = on_connect
    client.on_publish = on_publish

    try:
        # Connect to the broker. The last argument is the keepalive time in seconds.
        client.connect(BROKER_ADDRESS, BROKER_PORT, 60)

        # Start the network loop in a non-blocking thread.
        # This is necessary for callbacks (like on_connect) to fire.
        client.loop_start()

        # Keep the main thread alive briefly to allow the connection, publication,
        # and disconnection process to complete via the loop_start thread.
        print("Waiting for connection and publish to complete...")
        
        # Wait up to 5 seconds for the connection to be established
        timeout = 5
        while timeout > 0 and not client.is_connected():
            time.sleep(0.1)
            timeout -= 0.1
        
        if not client.is_connected():
            print("Failed to connect to the broker within timeout period")
            client.loop_stop()
            return
            
        # Wait for disconnection (which happens after successful publish)
        timeout = 5
        while timeout > 0 and client.is_connected():
            time.sleep(0.1)
            timeout -= 0.1

    except ConnectionRefusedError:
        print("FATAL ERROR: Connection refused. Is your MQTT broker running on localhost:1873?")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # Ensure the loop stops if it was started
        client.loop_stop()
        print("Client loop stopped. Script exit.")

if __name__ == "__main__":
    main()

