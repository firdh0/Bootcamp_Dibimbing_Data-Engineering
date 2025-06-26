import json
import time
import random
import uuid
import os
from kafka import KafkaProducer
from dotenv import load_dotenv
from pathlib import Path

try:
    dotenv_path = Path('/resources/.env')
    load_dotenv(dotenv_path=dotenv_path)
    print("The .env file is loaded.")
except Exception as e:
    print(f"Unable to load .env file: {e}. Using default values.")

KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = "9092"
KAFKA_BOOTSTRAP_SERVERS = f"{KAFKA_HOST}:{KAFKA_PORT}"
KAFKA_TOPIC = "purchase_events"

def get_purchase_event() -> None:
    """
    Generate a simulated random purchase event with realistic fields for data streaming.

    This function simulates a purchase transaction by returning a dictionary
    containing randomly generated values for each field. It is intended for use
    in data pipeline simulations, such as Kafka producers or stream processing systems.

    Returns:
        dict: A dictionary representing a single purchase event with the following keys:
            - purchase_id (str): A unique identifier for the purchase, generated using UUID.
            - product_id (int): An integer representing the ID of the product (range: 1–100).
            - price (float): The product's price in currency units (range: 10.0–500.0).
            - quantity (int): The number of items purchased (range: 1–5).
            - timestamp (float): A Unix timestamp indicating when the event occurred.

    Raises:
        None
    """
    
    return {
        "purchase_id": str(uuid.uuid4()),
        "product_id": random.randint(1, 100),
        "price": round(random.uniform(10.0, 500.0), 2),
        "quantity": random.randint(1, 5),
        "timestamp": time.time()  # Unix timestamp
    }

def json_serializer(data: dict) -> bytes:
    """
    Serialize a Python dictionary into JSON-encoded bytes.

    This function takes a Python dictionary and converts it into a JSON-formatted
    byte string using UTF-8 encoding. It is typically used when sending data
    to message brokers like Kafka that require binary-encoded payloads.

    Parameters:
        data (dict): The Python dictionary to serialize into JSON format.

    Returns:
        bytes: A UTF-8 encoded byte string representing the JSON-serialized dictionary.

    Raises:
        TypeError: If the input data cannot be serialized into JSON.
    """

    return json.dumps(data).encode("utf-8")

if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=json_serializer
    )

    print(f"Start sending data to the Kafka topic: {KAFKA_TOPIC}...")
    try:
        while True:
            purchase_event_data = get_purchase_event()
            print(f"Send: {purchase_event_data}")

            producer.send(KAFKA_TOPIC, purchase_event_data)
            
            time.sleep(random.uniform(1, 5)) 

    except KeyboardInterrupt:
        print("Delivery suspended.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Closing producer...")
        producer.close()