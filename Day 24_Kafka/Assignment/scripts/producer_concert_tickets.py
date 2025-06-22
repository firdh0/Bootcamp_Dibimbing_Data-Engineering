import os
import random
from time import sleep
import uuid
from faker import Faker

from pathlib import Path      # <-- TAMBAHKAN
from dotenv import load_dotenv  # <-- TAMBAHKAN

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer

# Import skema TicketPurchase yang sudah dicompile
import sys
sys.path.append('/resources/kafka')
import ticket_purchase_pb2

# --- Konfigurasi ---
# TAMBAHKAN BLOK INI UNTUK MEMUAT .env DARI PATH YANG BENAR DI DALAM KONTAINER
dotenv_path = Path('/resources/.env')
load_dotenv(dotenv_path=dotenv_path)

KAFKA_HOST = os.getenv("KAFKA_HOST")
SCHEMA_REGISTRY_URL = f"http://{os.getenv('SCHEMA_REG_HOST')}:8081"
KAFKA_TOPIC = "concert_ticket_purchases"

# Validasi apakah variabel berhasil dimuat
if not KAFKA_HOST or not SCHEMA_REGISTRY_URL:
    raise ValueError("Pastikan KAFKA_HOST dan SCHEMA_REGISTRY_URL sudah diatur di file .env")

# Inisialisasi Producer
producer = SerializingProducer({
    'bootstrap.servers': f'{KAFKA_HOST}:9092',
    'value.serializer': ProtobufSerializer(
        ticket_purchase_pb2.TicketPurchase,
        SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL}),
        {'use.deprecated.format': False}
    ),
    'key.serializer': StringSerializer('utf_8')
})

faker = Faker('id_ID') # Menggunakan data nama Indonesia

# Daftar harga tiket dari poster
ticket_prices = {
    "ACE": {"OFC Pre-Sale": 4800000},
    "KING": {"OFC Pre-Sale": 750000, "General": 800000},
    "QUEEN": {"OFC Pre-Sale": 500000, "General": 550000},
    "JACK": {"OFC Pre-Sale": 400000, "General": 450000}
}

# --- Loop Utama Producer ---
print(f"Starting ticket purchase simulation for topic '{KAFKA_TOPIC}'...")
while True:
    try:
        # Pilih tipe tiket secara acak
        selected_tier = random.choice(list(ticket_prices.keys()))
        
        # Pilih tipe pembelian secara acak
        purchase_options = ticket_prices[selected_tier]
        selected_purchase_type = random.choice(list(purchase_options.keys()))
        
        # Dapatkan harga yang sesuai
        price = purchase_options[selected_purchase_type]
        
        # Buat data event pembelian tiket
        purchase_event = ticket_purchase_pb2.TicketPurchase(
            purchase_id=str(uuid.uuid4()),
            user_id=str(faker.random_int(min=1000, max=9999)),
            user_name=faker.name(),
            ticket_tier=selected_tier,
            purchase_type=selected_purchase_type,
            price=price,
            quantity=random.choice([1, 2]), # Pembelian 1 atau 2 tiket
            purchase_timestamp=int(faker.unix_time())
        )
        
        print(f"New Purchase: {purchase_event.user_name} bought {purchase_event.quantity} {purchase_event.ticket_tier} ticket(s) for Rp{purchase_event.price}")
        
        # Kirim event ke Kafka
        producer.produce(
            topic=KAFKA_TOPIC,
            key=purchase_event.purchase_id,
            value=purchase_event
        )
        producer.poll(0) # Wajib untuk memicu pengiriman
        
        sleep(random.uniform(1, 4)) # Jeda acak 1-4 detik

    except KeyboardInterrupt:
        print("Stopping producer...")
        break
    except Exception as e:
        print(f"An error occurred: {e}")
        sleep(5)

producer.flush()