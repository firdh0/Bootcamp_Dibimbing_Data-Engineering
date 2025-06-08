# spark_utils.py

import os
import sys
from pyspark.sql import SparkSession

def create_spark_session(app_name="Day21_Assignment"):
    """
    Membuat dan mengembalikan SparkSession.
    Versi ini menggabungkan semua perbaikan:
    1. Pengaturan SPARK_HOME untuk menangani path dengan spasi.
    2. Pengaturan path Python di konfigurasi.
    3. Penambahan extraJavaOptions untuk kompatibilitas Java.

    Args:
        app_name (str): Nama aplikasi Spark.

    Returns:
        SparkSession: Objek SparkSession yang telah dibuat.
    """
    # 1. Mengatur SPARK_HOME secara manual untuk membantu Spark menemukan dirinya sendiri
    #    saat startup, yang seringkali gagal jika path mengandung spasi.
    #    Path ini menunjuk ke lokasi library pyspark di dalam lingkungan venv Anda.
    pyspark_path = os.path.join(os.path.dirname(sys.executable), '..', 'Lib', 'site-packages', 'pyspark')
    if os.path.isdir(pyspark_path):
        os.environ['SPARK_HOME'] = pyspark_path
        print(f"SPARK_HOME diatur secara manual ke: {pyspark_path}")
    else:
        print(f"Peringatan: Tidak dapat menemukan direktori pyspark di path yang diharapkan: {pyspark_path}")

    # 2. Mengatur path Python langsung di konfigurasi Spark.
    #    Ini memberitahu Spark executable Python mana yang harus digunakan untuk worker-nya.
    python_executable_path = sys.executable
    print(f"Mengatur path Python untuk Spark ke: {python_executable_path}")
    
    # 3. Menambahkan extraJavaOptions dari kode Anda untuk kompatibilitas Java.
    extra_java_options = (
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--enable-native-access=ALL-UNNAMED"
    )

    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.extraJavaOptions", extra_java_options) \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.pyspark.python", python_executable_path) \
        .config("spark.pyspark.driver.python", python_executable_path) \
        .getOrCreate()

if __name__ == '__main__':
    # Contoh penggunaan sederhana jika file ini dijalankan langsung
    spark = create_spark_session()
    print(f"SparkSession '{spark.sparkContext.appName}' berhasil dibuat dengan versi Spark {spark.version}")
    
    # Membuat DataFrame sederhana untuk memverifikasi worker berjalan
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    print("DataFrame uji coba berhasil dibuat:")
    df.show()
    
    spark.stop()
