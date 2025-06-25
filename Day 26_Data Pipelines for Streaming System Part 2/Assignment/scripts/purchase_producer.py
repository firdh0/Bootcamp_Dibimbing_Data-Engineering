import json
import time
import random
import uuid
import os
from kafka import KafkaProducer
from dotenv import load_dotenv
from pathlib import Path

# Memuat environment variables
# Diasumsikan skrip ini dijalankan dari dalam kontainer yang memiliki akses ke .env
try:
    dotenv_path = Path('/resources/.env')
    load_dotenv(dotenv_path=dotenv_path)
    print("File .env dimuat.")
except Exception as e:
    print(f"Tidak dapat memuat file .env: {e}. Menggunakan nilai default.")

# Konfigurasi Kafka
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = "9092"
KAFKA_BOOTSTRAP_SERVERS = f"{KAFKA_HOST}:{KAFKA_PORT}"
KAFKA_TOPIC = "purchase_events"

def get_purchase_event():
    """Menghasilkan satu event pembelian acak."""
    return {
        "purchase_id": str(uuid.uuid4()),
        "product_id": random.randint(1, 100),
        "price": round(random.uniform(10.0, 500.0), 2),
        "quantity": random.randint(1, 5),
        "timestamp": time.time()  # Unix timestamp
    }

def json_serializer(data):
    """Serializer untuk mengubah dictionary Python menjadi JSON bytes."""
    return json.dumps(data).encode("utf-8")

if __name__ == "__main__":
    # Inisialisasi Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=json_serializer
    )

    print(f"Memulai pengiriman data ke topik Kafka: {KAFKA_TOPIC}...")
    try:
        while True:
            # 1. Menghasilkan event pembelian acak
            purchase_event_data = get_purchase_event()
            print(f"Mengirim: {purchase_event_data}")

            # Mengirim event ke topik Kafka
            producer.send(KAFKA_TOPIC, purchase_event_data)
            
            # Memberi jeda acak sebelum mengirim event berikutnya
            time.sleep(random.uniform(1, 5)) 

    except KeyboardInterrupt:
        print("Pengiriman dihentikan.")
    except Exception as e:
        print(f"Terjadi error: {e}")
    finally:
        print("Menutup producer...")
        producer.close()