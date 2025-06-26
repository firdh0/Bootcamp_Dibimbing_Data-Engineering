# 🎫 Day 24 - Assignment [Data Streaming Pipeline: Concert Ticket Sales Analysis]
<br>
This project is an implementation of an end-to-end data pipeline to simulate, send, validate, and analyze concert ticket sales data in real time.

<br><br>
## 🏗️ Architecture & Technology

The data workflow for this project flows as follows:

**Python Producer → Protobuf Serialization → Schema Registry → Kafka Topic → KsqlDB (Stream & Table) → Real-Time Analysis**

### 🧰 Key Technologies Used:

- 🐳 **Containerization**: Docker & Docker Compose
- 🛠️ **Automation**: GNU Make (Makefile)  
- 📩 **Message Broker**: Apache Kafka (using the stack from Confluent Inc.)
- 🧬 **Schema Management**: Confluent Schema Registry
- 📦 **Serialization Format**: Google Protocol Buffers (Protobuf)
- 🔁 **Stream Processing**: KsqlDB  
- 🐍 **Language**: Python with the `confluent-kafka` library for Producer.

<br><br>

## 📁 Project Structure (Important Files)

The following is the essential file and directory structure for running this concert ticket pipeline:

```.

├── docker/
│   ├── Dockerfile.jupyter        # Blueprint for the Jupyter image
│   └── docker-compose-kafka.yml  # Infrastructure definition (Confluent Stack)
├── kafka/
│   ├── ticket_analytics.ksql.sql # KSQL analysis logic
│   ├── ticket_purchase.proto     # Protobuf data schema for tickets
│   └── ticket_purchase_pb2.py    # Python file compiled from .proto
├── scripts/
│   └── producer_concert_tickets.py # Script to generate ticket sales data
├── .env                          # Centralized configuration file
└── Makefile                      # Control center for all commands
```

<br><br>

## ⚙️ Configuration

### 🔐 `.env`
This file stores all configuration variables. Key variables relevant to this pipeline are:

```
# Internal Kafka address for inter-container connections
KAFKA_BOOTSTRAP_SERVERS_INTERNAL=dataeng-kafka:29092

# Internal Schema Registry address
SCHEMA_REGISTRY_URL=http://dataeng-schema-registry:8081
```

### 🧱 `docker/docker-compose-kafka.yml`
This file has been completely overhauled to use the full stack from Confluent Inc. This is the most crucial change to ensure full compatibility between Kafka, Schema Registry, and KsqlDB.

Main services running:  
🦓 zookeeper,  
🦄 kafka,  
📜 schema-registry,  
🔮 ksqldb-server,  
🧑‍💻 ksqldb-cli

Each service has been configured to communicate with each other via the Docker internal network.

### 📦 `docker/Dockerfile.jupyter`
Modified to add the Python library `confluent-kafka[protobuf]` and upgrade protobuf to resolve version compatibility issues.

### 🧩 `Makefile`
New targets have been added to simplify execution:

- `concert-ticket-produce`: Runs the ticket data producer script.
- `ksql-run-script`: Automatically executes all KSQL analysis files.

<br><br>

## 🔍 Workflow Details and Tool Functions

### 🎟️ Event Creation & Serialization (Producer & Protobuf)
The `producer_concert_tickets.py` script acts as a ticket sales application simulator. It creates random transaction data (such as user ID, ticket type, and price).

Before being sent, this data is converted from Python objects into a compact binary format using Protobuf, according to the schema in `ticket_purchase.proto`.

### 🧰 Schema Validation (Schema Registry)
When the producer sends data, it first communicates with the Schema Registry.  
This service acts as a “gatekeeper” that ensures all data sent to Kafka has a valid and consistent structure.

### 🛢️ Event Storage (Kafka)
After validation, the binary message is sent to the Kafka Broker and stored in the `concert_ticket_purchases` topic.  
Kafka functions as a reliable and scalable buffer.

### 📊 Real-Time Analysis (KsqlDB)
KsqlDB continuously monitors the `concert_ticket_purchases` topic. When a new message arrives, it:

- Contacts the Schema Registry to obtain the data schema.  
- Deserializes the binary message into structured data (rows and columns).
- This structured data is then processed by the `CREATE TABLE ... AS SELECT ...` query running in the background to continuously calculate total revenue.

<br><br>

## 🚀 How to Use

Here is a complete guide to running this project from scratch.

### ✅ Prerequisites

Make sure you have the following software installed on your computer:

- Docker & Docker Compose  
- GNU Make  
- Protocol Buffer Compiler (`protoc`)

---

### 🧪 Step 1: Preparation (Only Done Once)

🔧 Compile Protobuf:
```bash
protoc --proto_path=kafka --python_out=kafka kafka/ticket_purchase.proto
```

🐳 Build Docker Image:
```bash
make docker-build
```

---

### 🔄 Step 2: Run the Pipeline

🚀 Start Infrastructure:
```bash
make clean && make kafka && make jupyter
```

📦 Create Kafka Topic:
```bash
make kafka-create-topic topic=concert_ticket_purchases partition=3
```

🧑‍🎤 Run Producer:  
Open a new terminal and run:
```bash
make concert-ticket-produce
```

🔍 Run KSQL Analysis Script:  
In another terminal to view results, run:
```bash
make ksql-run-script
```

---

### 👀 Step 3: Monitor Results & Stop

📺 View Results (Optional):  
Log in to the KSQL CLI and run the `SELECT` query:

Log in to the CLI:
```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

In ksql>:
```sql
SELECT * FROM revenue_by_tier EMIT CHANGES;
```

🛑 Stopping the Project:

1. Press `Ctrl + C` in the terminal where the producer is running  
2. Stop all containers:
```bash
make clean
```

---