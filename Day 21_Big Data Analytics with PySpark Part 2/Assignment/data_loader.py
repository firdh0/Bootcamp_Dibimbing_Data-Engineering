import os
from pyspark.sql.types import StructType

# --- Fungsi Pemuat Data dari CSV ---
def load_csv_data(spark, file_path, header=True, infer_schema=True):
    """
    Fungsi generik untuk memuat data dari satu file CSV.

    Args:
        spark (SparkSession): SparkSession aktif.
        file_path (str): Path ke file CSV.
        header (bool): Apakah file CSV memiliki baris header.
        infer_schema (bool): Apakah Spark harus mencoba menyimpulkan skema secara otomatis.

    Returns:
        DataFrame: DataFrame Spark yang dimuat dari CSV, atau DataFrame kosong jika terjadi error.
    """
    print(f"Memuat data dari CSV: {file_path}")
    try:
        df = spark.read.csv(file_path, header=header, inferSchema=infer_schema)
        
        if not df.rdd.isEmpty(): # Cara yang lebih andal untuk memeriksa apakah DataFrame kosong
            print(f"Berhasil memuat {df.count()} baris dari {file_path}")
            return df
        else:
            print(f"Peringatan: Tidak ada data yang dimuat dari {file_path} atau file kosong.")
            return spark.createDataFrame([], StructType([]))
            
    except Exception as e:
        print(f"Error saat memuat CSV {file_path}: {e}")
        return spark.createDataFrame([], StructType([]))

def find_csv_files(directory):
    """
    Menemukan semua file .csv dalam sebuah direktori.

    Args:
        directory (str): Path ke direktori yang akan dipindai.

    Returns:
        list: Daftar path lengkap ke setiap file .csv yang ditemukan.
              Mengembalikan list kosong jika direktori tidak ada.
    """
    if not os.path.isdir(directory):
        print(f"Error: Direktori '{directory}' tidak ditemukan.")
        return []
    
    csv_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]
    print(f"Ditemukan {len(csv_files)} file CSV di direktori '{directory}': {csv_files}")
    return csv_files

def load_all_data_from_directory(spark, directory):
    """
    Memuat semua file CSV dari direktori yang ditentukan secara dinamis.
    Nama file (tanpa ekstensi .csv) akan digunakan sebagai key dalam kamus.

    Args:
        spark (SparkSession): SparkSession aktif.
        directory (str): Path ke direktori yang berisi file-file CSV.

    Returns:
        dict: Kamus di mana key adalah nama file dasar dan value adalah DataFrame yang sesuai.
    """
    csv_files = find_csv_files(directory)
    if not csv_files:
        print("Tidak ada file CSV untuk dimuat. Mengembalikan kamus kosong.")
        return {}
    
    dataframes = {}
    for file_path in csv_files:
        # Menggunakan nama file tanpa ekstensi sebagai key
        # Contoh: 'data/customer_flight_activity.csv' -> 'customer_flight_activity'
        key_name = os.path.splitext(os.path.basename(file_path))[0]
        df = load_csv_data(spark, file_path)
        if not df.rdd.isEmpty():
            dataframes[key_name] = df
    
    print(f"Berhasil memuat {len(dataframes)} DataFrame dari direktori '{directory}'.")
    return dataframes


if __name__ == '__main__':
    # Impor create_spark_session dari modul spark_utils
    try:
        from spark_utils import create_spark_session
    except ImportError:
        print("Pastikan spark_utils.py ada di direktori yang sama atau dalam PYTHONPATH.")
        from pyspark.sql import SparkSession
        def create_spark_session(app_name="FallbackSession"):
            return SparkSession.builder.appName(app_name).getOrCreate()

    spark_session = create_spark_session("DataLoaderDynamicTest")

    # Tentukan direktori data. Tidak perlu mendefinisikan setiap file lagi.
    data_directory = "data/"
    
    print(f"Mencoba memuat semua file CSV dari direktori: '{data_directory}'")

    # Memuat semua data dan mendapatkan kamus DataFrame
    loaded_dataframes = load_all_data_from_directory(spark_session, data_directory)

    # Memeriksa dan menampilkan setiap DataFrame yang berhasil dimuat
    if loaded_dataframes:
        print("\n--- Ringkasan DataFrame yang Dimuat ---")
        for name, df in loaded_dataframes.items():
            print(f"\nDataFrame: '{name}'")
            df.show(5)
            df.printSchema()
    else:
        print("\nTidak ada DataFrame yang dimuat.")

    spark_session.stop()
