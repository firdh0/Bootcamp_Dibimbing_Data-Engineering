import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, date_format, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, FloatType

KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = "9092"
KAFKA_BOOTSTRAP_SERVERS = f"{KAFKA_HOST}:{KAFKA_PORT}"
KAFKA_TOPIC = "purchase_events"

spark = (
    SparkSession.builder.appName("DailyPurchaseAggregator")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2")
    .config("spark.sql.shuffle.partitions", 4)
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("purchase_id", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("timestamp", DoubleType(), True)
])

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = (
    kafka_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_timestamp", col("timestamp").cast(TimestampType()))
    .withColumn("total_amount", col("price") * col("quantity"))
    .withColumn("purchase_date", date_format(col("event_timestamp"), "yyyy-MM-dd"))
)

daily_purchase_total = (
    parsed_df
    .groupBy("purchase_date")
    .agg(sum("total_amount").alias("daily_total"))
)

output_df = (
    daily_purchase_total
    .withColumn("timestamp", current_timestamp())
    .withColumnRenamed("daily_total", "running_total")
    .select("timestamp", "purchase_date", "running_total")
)

query = (
    output_df.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime="15 seconds") 
    .start()
)

query.awaitTermination()