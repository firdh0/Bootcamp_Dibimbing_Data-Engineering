import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, round as _round, count as _count
from dotenv import load_dotenv
from pathlib import Path

# --- 1. Inisialisasi Spark dan Konfigurasi ---

print("Initializing Spark Session and loading environment variables...")

# Memuat variabel dari file .env untuk koneksi ke PostgreSQL
# Path ini merujuk ke lokasi file di dalam kontainer Spark
dotenv_path = Path("/opt/app/.env") 
load_dotenv(dotenv_path=dotenv_path)

postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_container_name = os.getenv("POSTGRES_CONTAINER_NAME")
postgres_dw_db = os.getenv("POSTGRES_DW_DB") # Database 'warehouse'

# Inisialisasi SparkSession
# !! PERBAIKAN: Konfigurasi .config() untuk paket JARs dihapus.
# Konfigurasi ini sekarang ditangani oleh Airflow DAG.
spark = SparkSession.builder.appName("CustomerAnalyticsETL").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Spark session initialized successfully.")

# --- Langkah 3: Melakukan ETL dengan PySpark ---

# --- 3a. Extract ---
# Membaca data dari file Parquet yang telah Anda bersihkan.
# Ini adalah path standar di dalam kontainer setelah Anda menambahkan volume mount.
base_data_path = "/data/cleaned" 
path_loyalty = f"{base_data_path}/customer_loyalty_history"
path_activity = f"{base_data_path}/customer_flight_activity"
path_calendar = f"{base_data_path}/calendar"

try:
    df_loyalty = spark.read.parquet(path_loyalty)
    df_activity = spark.read.parquet(path_activity)
    df_calendar = spark.read.parquet(path_calendar) # Membaca data kalender
    print("Successfully read Parquet files.")
    print("Loyalty History Schema:")
    df_loyalty.printSchema()
    print("Flight Activity Schema:")
    df_activity.printSchema()
    print("Calendar Schema:")
    df_calendar.printSchema()
except Exception as e:
    print(f"Error reading Parquet files: {e}")
    spark.stop()
    exit(1)


# --- 3b. Transform ---
# Melakukan agregasi dan analisis. Contoh ini membuat ringkasan profil pelanggan.

print("Starting data transformation...")

# Gabungkan data aktivitas dengan data profil pelanggan berdasarkan 'loyalty_number'
df_customer_profile = df_activity.join(df_loyalty, "loyalty_number", "inner")

# Lakukan agregasi untuk membuat ringkasan per pelanggan
df_customer_summary = (
    df_customer_profile.groupBy(
        "loyalty_number", "country", "province", "city", 
        "gender", "education", "salary", "marital_status", "loyalty_card"
    )
    .agg(
        _sum("total_flights").alias("lifetime_total_flights"),
        _sum("distance").alias("lifetime_total_distance"),
        _sum("points_accumulated").alias("lifetime_points_accumulated"),
        _sum("points_redeemed").alias("lifetime_points_redeemed"),
        _round(_sum("dollar_cost_points_redeemed"), 2).alias("lifetime_dollar_cost_redeemed")
    )
    .orderBy(col("loyalty_number"))
)

# Tambah kolom baru untuk analisis, contoh: net_points
df_customer_summary = df_customer_summary.withColumn(
    "net_points",
    col("lifetime_points_accumulated") - col("lifetime_points_redeemed")
)

print("Transformation and aggregation complete. Showing summary preview:")
df_customer_summary.show(5)


# --- 3c. Load ---
# Menyimpan hasil analisis ke Data Warehouse PostgreSQL.

print("Preparing to load data into PostgreSQL...")

# Konfigurasi koneksi JDBC ke PostgreSQL
# Membuat tabel bernama 'customer_summary' di dalam skema 'public' (default)
table_name = "public.customer_summary"
jdbc_url = f"jdbc:postgresql://{postgres_container_name}/{postgres_dw_db}"
jdbc_properties = {
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver",
}

try:
    print(f"Writing data to PostgreSQL table: {table_name}")
    # Menggunakan mode "overwrite" untuk mengganti data lama setiap kali DAG berjalan
    df_customer_summary.write.mode("overwrite").jdbc(
        url=jdbc_url, table=table_name, properties=jdbc_properties
    )
    print("Successfully loaded data into PostgreSQL.")
except Exception as e:
    print(f"Error writing to PostgreSQL: {e}")
    spark.stop()
    exit(1)


# --- Langkah 4: Membaca Data dari PostgreSQL dengan PySpark ---
# Langkah ini untuk verifikasi dan memenuhi syarat tugas nomor 4.

try:
    print("\nReading data back from PostgreSQL for verification...")
    df_read_from_pg = spark.read.jdbc(
        url=jdbc_url, table=table_name, properties=jdbc_properties
    )
    
    print("Data successfully read from PostgreSQL. Showing top 10 rows from the table:")
    df_read_from_pg.show(10)

    # Menampilkan jumlah baris untuk memastikan data konsisten
    count = df_read_from_pg.count()
    print(f"Total rows in '{table_name}': {count}")

except Exception as e:
    print(f"Error reading back from PostgreSQL: {e}")


# --- Selesai ---
spark.stop()
print("\nETL job finished successfully.")
