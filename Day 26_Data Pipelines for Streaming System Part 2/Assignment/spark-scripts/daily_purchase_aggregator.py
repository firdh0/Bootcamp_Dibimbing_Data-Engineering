import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, date_format, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, FloatType

# Konfigurasi koneksi ke Kafka dari environment variables
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = "9092"
KAFKA_BOOTSTRAP_SERVERS = f"{KAFKA_HOST}:{KAFKA_PORT}"
KAFKA_TOPIC = "purchase_events"

# 1. Inisialisasi Spark Session dengan paket Kafka
spark = (
    SparkSession.builder.appName("DailyPurchaseAggregator")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2")
    .config("spark.sql.shuffle.partitions", 4)
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 2. Definisikan skema untuk data JSON dari Kafka
schema = StructType([
    StructField("purchase_id", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("timestamp", DoubleType(), True)
])

# 3. Baca data sebagai stream dari Kafka
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# 4. Parse JSON dan lakukan transformasi
# Tambahkan kolom total_amount dan purchase_date
parsed_df = (
    kafka_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_timestamp", col("timestamp").cast(TimestampType()))
    .withColumn("total_amount", col("price") * col("quantity"))
    .withColumn("purchase_date", date_format(col("event_timestamp"), "yyyy-MM-dd"))
)

# 5. Lakukan agregasi untuk menghitung total pembelian harian
daily_purchase_total = (
    parsed_df
    .groupBy("purchase_date")
    .agg(sum("total_amount").alias("daily_total"))
)

# 6. Siapkan output data sesuai format yang diminta
# Menggunakan outputMode("complete") akan menampilkan semua hasil agregasi setiap trigger
output_df = (
    daily_purchase_total
    .withColumn("timestamp", current_timestamp())
    .withColumnRenamed("daily_total", "running_total")
    .select("timestamp", "purchase_date", "running_total")
)

# 7. Tulis output ke konsol
# Menggunakan mode 'complete' seperti yang diminta
query = (
    output_df.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime="15 seconds") # Trigger serupa dengan contoh sebelumnya
    .start()
)

query.awaitTermination()