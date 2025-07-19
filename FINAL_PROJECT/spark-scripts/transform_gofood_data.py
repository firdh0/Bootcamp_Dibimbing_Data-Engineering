import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, when, lit, monotonically_increasing_id, to_date, 
    dayofmonth, date_format, month, quarter, year, explode, split, 
    trim, lower, expr, least, greatest
)
from pyspark.sql.types import StringType, FloatType

def create_spark_session():
    """Membuat dan mengembalikan Spark Session."""
    return (
        SparkSession.builder
        .appName("GofoodETLTransformation")
        .master("local[*]") # Menggunakan semua core yang tersedia secara lokal
        .getOrCreate()
    )

def categorize_menu_udf():
    """Membuat UDF untuk mengkategorikan menu menjadi Makanan atau Minuman."""
    def categorize_menu(menu_name):
        if menu_name is None:
            return "Lain-lain"
        menu_name_lower = menu_name.lower()
        drink_keywords = ['es', 'jus', 'teh', 'kopi', 'susu', 'cokelat', 'float', 'boba', 'latte', 'yakult', 'soda', 'air']
        for keyword in drink_keywords:
            if keyword in menu_name_lower:
                return "Minuman"
        return "Makanan"
    return udf(categorize_menu, StringType())

def parse_promo_udf():
    """Membuat UDF untuk parsing teks promo menggunakan regex."""
    def parse_promo_text(text, pattern):
        if text is None:
            return 0.0
        match = re.search(pattern, text)
        if match:
            extracted_group = match.group(1)
            clean_match = extracted_group.lower().replace('rb', '000').replace('.', '').replace(',', '')
            if clean_match.isdigit():
                return float(clean_match)
        return 0.0
    return udf(parse_promo_text, FloatType())

def main():
    """Fungsi utama untuk menjalankan proses ETL."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # --- 1. BACA DATA (EXTRACT) ---
    print("Membaca data CSV dari folder /data...")
    base_path = "/data" # Path di dalam kontainer
    
    # Opsi untuk membaca CSV dengan benar
    csv_options = {"header": True, "multiLine": True, "inferSchema": True, "escape": '"'}

    restoran_df = spark.read.csv(os.path.join(base_path, "hasil_detail_restoran.csv"), **csv_options)
    menu_df = spark.read.csv(os.path.join(base_path, "hasil_menu_restoran.csv"), **csv_options)
    promo_df = spark.read.csv(os.path.join(base_path, "hasil_promo_restoran.csv"), **csv_options)
    ulasan_df = spark.read.csv(os.path.join(base_path, "hasil_ulasan_pelanggan.csv"), **csv_options)

    # --- 2. PEMBERSIHAN DATA (TRANSFORM) ---
    print("Membersihkan data dari duplikat...")
    restoran_df = restoran_df.dropDuplicates()
    menu_df = menu_df.dropDuplicates()
    promo_df = promo_df.dropDuplicates()
    ulasan_df = ulasan_df.dropDuplicates()
    
    menu_df = menu_df.na.fill({"Detail Menu": "Tidak ada detail"})
    ulasan_df = ulasan_df.na.fill({"Produk yang Dibeli": "Produk tidak diketahui"})
    print("Pembersihan data selesai.")

    # --- 3. MEMBUAT TABEL DIMENSI (TRANSFORM) ---
    
    # === DIM_RESTAURANT ===
    print("Membuat Dim_Restaurant...")
    total_reviews_per_resto = ulasan_df.groupBy("Nama Restoran").count().withColumnRenamed("count", "total_reviews")
    
    dim_restaurant = restoran_df.select(
        col("Nama Restoran").alias("name"),
        col("Rating").alias("avg_rating"),
        col("Jarak").alias("distance"),
        col("Alamat").alias("address"),
        col("Detail Harga").alias("price_range")
    ).distinct()
    
    extract_city_udf = udf(lambda address: address.split(',')[-1].strip() if address and ',' in address else "Unknown", StringType())
    dim_restaurant = dim_restaurant.withColumn("city", extract_city_udf(col("address")))
    
    dim_restaurant = dim_restaurant.join(total_reviews_per_resto, dim_restaurant.name == total_reviews_per_resto["Nama Restoran"], "left").drop(total_reviews_per_resto["Nama Restoran"])
    dim_restaurant = dim_restaurant.withColumn("id_restaurant", monotonically_increasing_id())
    dim_restaurant = dim_restaurant.select("id_restaurant", "name", "avg_rating", "total_reviews", "distance", "address", "city", "price_range")

    # === DIM_MENU ===
    print("Membuat Dim_Menu...")
    dim_menu = menu_df.select(
        col("Nama Menu").alias("name"),
        col("Detail Menu").alias("description")
    ).distinct()
    dim_menu = dim_menu.withColumn("category", categorize_menu_udf()(col("name")))
    dim_menu = dim_menu.withColumn("id_menu", monotonically_increasing_id())
    dim_menu = dim_menu.select("id_menu", "name", "category", "description")

    # === DIM_PROMOTION ===
    print("Membuat Dim_Promotion...")
    dim_promotion = promo_df.select(
        col("Judul Promo").alias("name"),
        col("Detail Promo").alias("description")
    ).distinct()
    
    parse_promo = parse_promo_udf()
    dim_promotion = dim_promotion.withColumn("discount_percentage", parse_promo(col("name"), lit(r"(\d+)%")) / 100)
    dim_promotion = dim_promotion.withColumn("max_discount", parse_promo(col("name"), lit(r"maks\. (\d+[.,]?\d*rb|\d+[.,]?\d*)")))
    dim_promotion = dim_promotion.withColumn("min_purchase", parse_promo(col("description"), lit(r"Min\. pembelian (\d+[.,]?\d*rb|\d+[.,]?\d*)")))
    dim_promotion = dim_promotion.withColumn("delivery_discount", parse_promo(col("description"), lit(r"diskon ongkir (\d+[.,]?\d*rb|\d+[.,]?\d*)")))
    dim_promotion = dim_promotion.withColumn("id_promo", monotonically_increasing_id())
    dim_promotion = dim_promotion.select("id_promo", "name", "description", "discount_percentage", "max_discount", "min_purchase", "delivery_discount")

    # === DIM_TIME ===
    print("Membuat Dim_Time...")
    date_df = ulasan_df.withColumn("date_sql", to_date(col("Tanggal Beli"), "d MMMM yyyy")).select("date_sql").distinct().na.drop()
    dim_time = date_df.withColumn("id_time", monotonically_increasing_id()) \
        .withColumn("date", col("date_sql")) \
        .withColumn("day", dayofmonth(col("date_sql"))) \
        .withColumn("day_name", date_format(col("date_sql"), "EEEE")) \
        .withColumn("month", month(col("date_sql"))) \
        .withColumn("month_name", date_format(col("date_sql"), "MMMM")) \
        .withColumn("quarter", quarter(col("date_sql"))) \
        .withColumn("year", year(col("date_sql"))) \
        .withColumn("is_weekend", when(date_format(col("date_sql"), "E").isin(["Sat", "Sun"]), True).otherwise(False))
    dim_time = dim_time.select("id_time", "date", "day", "day_name", "month", "month_name", "quarter", "year", "is_weekend")
    
    print("Pembuatan tabel dimensi selesai.")

    # --- 4. SIMPAN HASIL (LOAD) ---
    print("Menyimpan tabel dimensi dan fakta dalam format Parquet...")
    output_path = "/data/processed" # Path di dalam kontainer
    
    dim_restaurant.write.mode("overwrite").parquet(os.path.join(output_path, "dim_restaurant.parquet"))
    dim_menu.write.mode("overwrite").parquet(os.path.join(output_path, "dim_menu.parquet"))
    dim_promotion.write.mode("overwrite").parquet(os.path.join(output_path, "dim_promotion.parquet"))
    dim_time.write.mode("overwrite").parquet(os.path.join(output_path, "dim_time.parquet"))
    
    print(f"✅ Semua data berhasil disimpan di {output_path}")
    spark.stop()

if __name__ == "__main__":
    main()

