# 🚀 Day 26 - Assignment [Streaming for Data Pipeline with PySpark]

This documentation explains the implementation of a simple streaming data pipeline using Apache Kafka and Apache Spark. The goal is to consume a stream of purchase event data, perform real-time daily aggregation, and display the results to the console.

---

## 🏗️ Architecture & Technology

The data workflow for this task is as follows:

**Python Producer (JSON) → Kafka Topic → PySpark Streaming Job (Daily Aggregation) → Console Output**

### 🧰 Key Technologies Used:

- 🐳 **Containerization**: Docker & Docker Compose  
- 🛠️ **Automation**: GNU Make (Makefile)
- 📩 **Message Broker**: Apache Kafka
- 🔁 **Stream Processing**: Spark Structured Streaming (PySpark)
- 🐍 **Language**: Python with the `kafka-python` library for the Producer.

---

## 📁 Files Used

Here are the key files used to complete this task:

- `scripts/purchase_event_producer.py`: Python script that acts as a data producer, generating random purchase events and sending them to Kafka in JSON format.  
- `spark-scripts/daily_purchase_aggregator.py`: PySpark streaming job that acts as a consumer. This script reads data from Kafka, performs aggregation, and displays the results.  
- `docker/docker-compose-kafka.yml`: Used to run the Kafka service.  
- `docker/docker-compose-spark.yml`: Used to run the Spark cluster (Master and Worker).  
- `.env`: Stores all configuration variables such as hostname and port.  
- `Makefile`: Provides short commands to run the producer and consumer.

---

## 🔍 Component Details & Workflow

### 1. `purchase_event_producer.py` (Producer)

**Function**: Simulates an application that generates purchase transaction data.

**Process**:

- Runs in an infinite loop.  
- Every few seconds, it creates a purchase data containing `purchase_id`, `product_id`, `price`, `quantity`, and `timestamp`.
- This data is converted to JSON format and sent to a Kafka topic named `purchase_events`.

### 2. `daily_purchase_aggregator.py` (PySpark Consumer & Aggregator)

**Function**: Processes data streams from Kafka to generate business insights (daily purchase totals).  

**Process**:

- **Reading from Kafka**: Using `spark.readStream`, this job connects to the `purchase_events` topic and begins consuming data in real time.
- **Parsing & Transformation**:  
  - Each incoming message (in JSON format) is parsed according to a predefined schema.  
  - Then, the script adds two new columns:  
    - `total_amount`: Result of `price * quantity`.  
    - `purchase_date`: Date of the transaction, extracted from `timestamp`.  
- **Daily Aggregation**:  
  - Using `groupBy(“purchase_date”)`, the data is grouped by date.  
  - The `sum(“total_amount”)` function is run to calculate the total sales for each date.  
- **Writing to the Console**:  
  - The aggregation results (total sales for each day) are displayed to the console every 15 seconds.  
  - The `complete` output mode is used, which means the entire aggregation result table will be reprinted at each trigger, showing the “running total” for each day that has been processed.

---

## ⚙️ How to Use

Here are the steps to run this pipeline.

### 🧪 Step 1: Run the Infrastructure

- Run Kafka:
```bash
  make kafka
  ```

- Run Spark:
```bash
  make spark
  ```

### 📦 Step 2: Set Up Kafka Topics

- Create Topic:  
  ```bash
  make kafka-create-topic topic=purchase_events partition=1
  ```

### 🔄 Step 3: Run Pipeline

- Run Producer:  
  Open a new terminal and run the producer. Leave this terminal running to continue sending data.  
  ```bash
  make produce-purchases
  ```  
  > Note: The `produce-purchases` target needs to be added to your `Makefile` as we discussed earlier.

- Run the Streaming Job (Consumer):  
  Open another new terminal and run the PySpark job.  
  ```bash
  make consume-daily-aggregation
  ```

---

## 📊 Example Output

After the PySpark job runs, you will see the output in the terminal updated every 15 seconds, something like this:

![Result](./images/Screenshot%202025-06-25%20231606.png)
```

📈 The `running_total` column will continue to increase as new purchase data for that date is entered.