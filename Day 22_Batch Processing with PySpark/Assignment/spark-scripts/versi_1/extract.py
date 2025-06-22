from pyspark.sql import SparkSession, DataFrame

def extract_data(spark: SparkSession) -> dict[str, DataFrame]:
    """
    Automatically reads all datasets from the /data/cleaned directory and returns 
    them as a dictionary of DataFrames.

    This function uses the Hadoop API provided by Spark to list all subdirectories
    under /data/cleaned. Each subdirectory is assumed to represent a dataset, and
    its contents are loaded as a Spark DataFrame (Parquet format).

    Parameters:
        spark (SparkSession): The active SparkSession used for reading the data.

    Returns:
        dict[str, DataFrame]: A dictionary where each key is the dataset name 
        (subdirectory name) and the value is the corresponding Spark DataFrame.

    Raises:
        FileNotFoundError: If no subdirectories (datasets) are found in /data/cleaned.
        Exception: If any error occurs during the reading of the Parquet files.
    """

    print("Extracting all datasets automatically from /data/cleaned...")
    base_data_path = "/data/cleaned"
    dataframes = {}
    
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    list_status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(base_data_path))
    
    dataset_names = [f.getPath().getName() for f in list_status if f.isDirectory()]
    
    if not dataset_names:
        raise FileNotFoundError(f"No subdirectories found in {base_data_path}")

    print(f"Found datasets: {dataset_names}")

    try:
        for dataset_name in dataset_names:
            path = f"{base_data_path}/{dataset_name}"
            print(f"Reading from: {path}")
            dataframes[dataset_name] = spark.read.parquet(path)
        print("Successfully read all source Parquet files.")
        return dataframes
    except Exception as e:
        print(f"Error during dynamic Extract phase: {e}")
        raise