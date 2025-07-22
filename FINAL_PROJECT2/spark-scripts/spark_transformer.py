from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, monotonically_increasing_id, udf, regexp_extract, when, lower, trim,
    to_date, date_format, dayofmonth, month, quarter, year, expr, split, explode, lit,
    avg, sum as _sum, dense_rank, count, least, regexp_replace, coalesce, max as spark_max,
    current_date, date_add, to_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, FloatType, IntegerType, LongType, DateType, TimestampType
from datetime import datetime

class SparkTransformer:
    """
    A class designed to perform ETL (Extract, Transform, Load) operations on restaurant
    and menu data using Apache Spark. It reads raw data from Google Cloud Storage (GCS),
    cleans and transforms it into dimensional and fact tables, and then saves the
    transformed data back to GCS in Parquet format, preparing it for a BigQuery
    data warehouse.

    This transformer handles data cleaning, feature engineering (e.g., extracting city
    from address, categorizing menu items), and generating surrogate keys for
    dimensional tables, ensuring data integrity and readiness for analytical purposes.

    Attributes:
        spark (SparkSession): The active SparkSession instance.
        gcs_input_path (str): The GCS path where raw input CSV files are located.
        gcs_output_path (str): The GCS path where transformed Parquet files will be saved.
        bigquery_project_id (str): The Google Cloud Project ID for BigQuery.
        bigquery_dataset_id (str): The BigQuery dataset ID where target tables reside.
        run_date (datetime.date): The specific date for which the data transformation
                                  is being run, derived from `run_date_str`.
        datasets (dict): A dictionary holding the raw Spark DataFrames loaded from GCS.

    Methods:
        _read_datasets(): Reads raw CSV data from specified GCS paths into Spark DataFrames.
        _clean_data(): Performs initial data cleaning steps like dropping duplicates
                       and handling null values.
        _get_max_id_from_bigquery(): Retrieves the maximum existing ID from a BigQuery
                                     table to ensure unique surrogate key generation.
        _create_dim_restaurant(): Creates the Dim_Restaurant DataFrame.
        _create_dim_menu(): Creates the Dim_Menu DataFrame.
        _create_dim_promotion(): Creates the Dim_Promotion DataFrame.
        _create_dim_date(): Creates the Dim_Date DataFrame.
        _create_fact_transaction(): Creates the Fact_Transaction DataFrame by joining
                                    dimensional tables.
        _save_tables(): Saves the transformed DataFrames to GCS in Parquet format.
        run(): Orchestrates the entire ETL process.
    """


    def __init__(self, spark: SparkSession, gcs_input_path: str, gcs_output_path: str,
                 bigquery_project_id: str, bigquery_dataset_id: str, run_date_str: str) -> None:
        """
        Initializes the SparkTransformer by setting up paths, Spark session, project IDs, 
        and reading datasets from GCS.

        Parameters:
            spark (SparkSession): Active Spark session.
            gcs_input_path (str): GCS path where raw CSVs are stored.
            gcs_output_path (str): GCS path where output tables will be saved.
            bigquery_project_id (str): GCP project ID for BigQuery operations.
            bigquery_dataset_id (str): BigQuery dataset ID.
            run_date_str (str): Run date in 'YYYY-MM-DD' format, used for partitioning and promo expiry.
        """
        self.spark = spark
        self.gcs_input_path = gcs_input_path
        self.gcs_output_path = gcs_output_path
        self.bigquery_project_id = bigquery_project_id
        self.bigquery_dataset_id = bigquery_dataset_id
        try:
            self.run_date = datetime.strptime(run_date_str.split('/')[0], '%Y-%m-%d').date()
        except ValueError:
            self.run_date = datetime.strptime(run_date_str, '%Y-%m-%d').date()
        print(f"SparkTransformer initialized with run_date: {self.run_date}")

        self.datasets = self._read_datasets()
        self._clean_data()


    def _read_datasets(self) -> dict:
        """
        Reads datasets from GCS input path into Spark DataFrames. Supports reading CSVs 
        and folders for customer reviews.

        Returns:
            dict: A dictionary where keys are dataset names and values are corresponding DataFrames.
        """
        print(f"Membaca dataset dari GCS path: {self.gcs_input_path}")
        dataset_files = {
            'restoran_df': 'hasil_detail_restoran.csv',
            'menu_df': 'hasil_menu_restoran.csv',
            'promo_df': 'hasil_promo_restoran.csv',
            'ulasan_df': 'ulasan_pelanggan/' # Membaca dari folder
        }
        datasets = {}
        for key, file_name in dataset_files.items():
            path = f"{self.gcs_input_path}/{file_name}"
            datasets[key] = self.spark.read.csv(path, header=True, inferSchema=True)
            print(f"Successfully loaded: {key} from {path}")
            # print(f"DEBUG: {key} count after initial read: {datasets[key].count()}")
        return datasets


    def _clean_data(self) -> None:
        """
        Performs initial data cleaning on the loaded datasets.

        This method iterates through all DataFrames in `self.datasets` and applies
        common cleaning steps:
            1. Drops duplicate rows from each DataFrame.
            2. Fills null values in specific columns:
                - "Detail Menu" in 'menu_df' with "Tidak ada detail".
                - "Produk yang Dibeli" in 'ulasan_df' with "Produk tidak diketahui".
            3. Converts the "Rating" column in 'ulasan_df' to FloatType, replacing
                comma decimal separators with dots, and fills any resulting nulls with 0.0.
            4. Casts "Tanggal Beli" in 'ulasan_df' to StringType to ensure consistent parsing later.
        """
        print("\n--- Starting the Data Cleaning Process ---")
        for name, df in self.datasets.items():
            original_count = df.count()
            df_cleaned = df.drop_duplicates()
            cleaned_count = df_cleaned.count()
            if original_count > cleaned_count:
                print(f"Dataset ‘{name}’: Removing {original_count - cleaned_count} duplicate rows.")
            self.datasets[name] = df_cleaned
            # print(f"DEBUG: {name} count after drop_duplicates: {self.datasets[name].count()}")

        if 'menu_df' in self.datasets:
            self.datasets['menu_df'] = self.datasets['menu_df'].na.fill("Tidak ada detail", subset=["Detail Menu"])
            print("Dataset ‘menu_df’: Null values in ‘Menu Details’ have been handled.")

        if 'ulasan_df' in self.datasets:
            self.datasets['ulasan_df'] = self.datasets['ulasan_df'].na.fill("Produk tidak diketahui", subset=["Produk yang Dibeli"])
            print("Dataset ‘review_df’: Null values in ‘Product Purchased’ have been handled.")
            
            if 'Rating' in self.datasets['ulasan_df'].columns:
                self.datasets['ulasan_df'] = self.datasets['ulasan_df'].withColumn(
                    "Rating", regexp_replace(col("Rating"), ",", ".").cast(FloatType())
                ).na.fill(0.0, subset=["Rating"])
                # print("DEBUG: ulasan_df 'Rating' column cast to FloatType and nulls filled.")
            
            if 'Tanggal Beli' in self.datasets['ulasan_df'].columns:
                self.datasets['ulasan_df'] = self.datasets['ulasan_df'].withColumn(
                    "Tanggal Beli", col("Tanggal Beli").cast(StringType())
                )
                # print("DEBUG: ulasan_df 'Tanggal Beli' column cast to StringType.")
            
        print("--- Data Cleaning Process Complete ---")


    def _get_max_id_from_bigquery(self, table_name: str, id_column: str) -> int:
        """
        Retrieves the maximum existing ID from a specified BigQuery table and column.

        This is used to ensure that newly generated surrogate keys in Spark
        do not conflict with existing IDs in BigQuery, allowing for incremental
        loading or safe overwrites. If the table does not exist or is empty,
        it returns 0.

        Parameters:
            table_name (str): The name of the BigQuery table (e.g., "dim_restaurant").
            id_column (str): The name of the ID column in the BigQuery table
                             (e.g., "id_restaurant").

        Returns:
            int: The maximum ID found in the specified BigQuery table's ID column,
                 or 0 if the table is empty or does not exist.
        """
        bq_table_full_path = f"{self.bigquery_project_id}.{self.bigquery_dataset_id}.{table_name}"
        print(f"Retrieving the maximum ID from BigQuery: {bq_table_full_path}.{id_column}")
        try:
            max_id_df = self.spark.read.format("bigquery") \
                .option("project", self.bigquery_project_id) \
                .option("dataset", self.bigquery_dataset_id) \
                .option("table", table_name) \
                .load() \
                .agg(spark_max(col(id_column))).collect()
            
            max_id = max_id_df[0][0] if max_id_df and max_id_df[0][0] is not None else 0
            print(f"ID maksimum dari {table_name}: {max_id}")
            return max_id
        except Exception as e:
            print(f"⚠️ Failed to retrieve the maximum ID from BigQuery for {table_name}. The table may not exist yet. Error: {e}")
            return 0


    def _create_dim_restaurant(self) -> DataFrame:
        """
        Creates the 'Dim_Restaurant' dimensional table.

        This method processes the 'restoran_df' and 'ulasan_df' datasets to
        create a dimension table for restaurants. It extracts relevant attributes
        like name, rating, distance, address, and price range. It also derives
        the 'city' from the address and calculates the total number of reviews
        per restaurant. Finally, it generates unique surrogate keys ('id_restaurant')
        by incorporating the maximum existing ID from BigQuery.

        Returns:
            DataFrame: A Spark DataFrame representing the 'Dim_Restaurant' table
                       with columns: 'id_restaurant', 'name', 'avg_rating',
                       'total_reviews', 'distance', 'address', 'city', 'price_range'.
        """
        print("\nCreate Dim_Restaurant...")
        detail_df = self.datasets['restoran_df']
        ulasan_df = self.datasets['ulasan_df']
        
        dim_restaurant = detail_df.select(
            col("Nama Restoran").alias("name"),
            col("Rating").alias("avg_rating"),
            col("Jarak").alias("distance"),
            col("Alamat").alias("address"),
            col("Detail Harga").alias("price_range")
        ).distinct()

        def extract_city(address):
            if address:
                parts = address.split(',')
                if len(parts) > 1:
                    return parts[-1].strip()
            return "Unknown"
        extract_city_udf = udf(extract_city, StringType())
        dim_restaurant = dim_restaurant.withColumn("city", extract_city_udf(col("address")))

        total_reviews_per_resto = ulasan_df.groupBy("Nama Restoran").count().withColumnRenamed("count", "total_reviews")
        dim_restaurant = dim_restaurant.join(total_reviews_per_resto, dim_restaurant.name == total_reviews_per_resto["Nama Restoran"], "left") \
            .drop(total_reviews_per_resto["Nama Restoran"])

        max_id_existing = self._get_max_id_from_bigquery("dim_restaurant", "id_restaurant")
        
        window_spec = Window.orderBy(monotonically_increasing_id())
        dim_restaurant = dim_restaurant.withColumn("row_num", dense_rank().over(window_spec).cast(LongType()))
        dim_restaurant = dim_restaurant.withColumn("id_restaurant", col("row_num") + lit(max_id_existing)) \
                                     .drop("row_num")
        
        return dim_restaurant.select("id_restaurant", "name", "avg_rating", "total_reviews", "distance", "address", "city", "price_range")


    def _create_dim_menu(self) -> DataFrame:
        """
        Creates the 'Dim_Menu' dimensional table.

        This method processes the 'ulasan_df' dataset to extract individual menu items
        from the 'Produk yang Dibeli' column (which can contain multiple items separated
        by " - "). It then calculates the average rating for each menu item,
        categorizes menu items as "Makanan" or "Minuman", and generates unique
        surrogate keys ('id_menu') by incorporating the maximum existing ID from BigQuery.

        Returns:
            DataFrame: A Spark DataFrame representing the 'Dim_Menu' table
                       with columns: 'id_menu', 'name', 'category', 'description', 'avg_rating'.
        """
        print("Create Dim_Menu...")
        ulasan_df = self.datasets['ulasan_df']
        # print(f"DEBUG: _create_dim_menu - ulasan_df count: {ulasan_df.count()}")
        
        individual_menu_df = ulasan_df.withColumn(
            "individual_menu",
            explode(split(col("Produk yang Dibeli"), " - "))
        )
        individual_menu_df = individual_menu_df.withColumn("individual_menu", trim(col("individual_menu")))
        individual_menu_df = individual_menu_df.filter(col("individual_menu").isNotNull() & (col("individual_menu") != ""))

        # print(f"DEBUG: _create_dim_menu - individual_menu_df count after explode/split/filter: {individual_menu_df.count()}")

        individual_menu_df = individual_menu_df.withColumn(
            "Rating", col("Rating").cast(FloatType())
        )
        
        avg_rating_df = individual_menu_df.groupBy("individual_menu").agg(avg("Rating").alias("avg_rating"))
        # print(f"DEBUG: _create_dim_menu - avg_rating_df count: {avg_rating_df.count()}")


        menu_df2 = individual_menu_df.select(
            col("individual_menu").alias("Nama Menu"),
            col("individual_menu").alias("Detail Menu")
        ).distinct()
        dim_menu = menu_df2.select(
            col("Nama Menu").alias("name"),
            col("Detail Menu").alias("description")
        ).distinct()
        
        # print(f"DEBUG: _create_dim_menu - dim_menu count after initial distinct: {dim_menu.count()}")

        def categorize_menu(menu_name):
            if menu_name is None:
                return "Lain-lain"
            menu_name = menu_name.lower()
            drink_keywords = ['es', 'jus', 'teh', 'kopi', 'susu', 'cokelat', 'float', 'boba', 'latte', 'yakult', 'soda', 'air']
            for keyword in drink_keywords:
                if keyword in menu_name:
                    return "Minuman"
            return "Makanan"
        categorize_menu_udf = udf(categorize_menu, StringType())
        dim_menu = dim_menu.withColumn("category", categorize_menu_udf(col("name")))
        dim_menu = dim_menu.join(
            avg_rating_df,
            dim_menu.name == avg_rating_df["individual_menu"],
            "left"
        )
        dim_menu = dim_menu.na.fill(0, subset=['avg_rating'])
        
        # print(f"DEBUG: _create_dim_menu - dim_menu count before ID generation: {dim_menu.count()}")

        max_id_existing = self._get_max_id_from_bigquery("dim_menu", "id_menu")
        
        window_spec = Window.orderBy(monotonically_increasing_id())
        dim_menu = dim_menu.withColumn("row_num", dense_rank().over(window_spec).cast(LongType()))
        dim_menu = dim_menu.withColumn("id_menu", col("row_num") + lit(max_id_existing)) \
                           .drop("row_num")
        
        # print(f"DEBUG: _create_dim_menu - dim_menu final count: {dim_menu.count()}")
        return dim_menu.select("id_menu", "name", "category", "description", "avg_rating")


    def _create_dim_promotion(self) -> DataFrame:
        """
        Creates the 'Dim_Promotion' dimensional table.

        This method processes the 'promo_df' dataset to extract various promotion
        details using regular expressions, such as discount percentage, maximum discount
        amount, minimum purchase requirement, additional food discount, and delivery discount.
        It converts extracted values to appropriate numeric types and handles nulls.
        It also sets a 'status' and 'active_until_date' based on the run date.
        Finally, it generates unique surrogate keys ('id_promo') by incorporating
        the maximum existing ID from BigQuery.

        Returns:
            DataFrame: A Spark DataFrame representing the 'Dim_Promotion' table
                       with columns: 'id_promo', 'name', 'description',
                       'discount_percentage', 'max_discount', 'min_purchase',
                       'additional_discount', 'delivery_discount', 'status', 'active_until_date'.
        """
        print("Create Dim_Promotion...")
        promo_df = self.datasets['promo_df']
        # print(f"DEBUG: _create_dim_promotion - promo_df count: {promo_df.count()}")
        
        discount_pattern = r"(\d+)%"
        max_discount_pattern = r"maks\. (\d+[.,]?\d*rb|\d+[.,]?\d*)"
        min_purchase_pattern = r"Min\. pembelian (\d+[.,]?\d*rb|\d+[.,]?\d*)"
        additional_discount_pattern = r"diskon makanan (\d+[.,]?\d*rb|\d+[.,]?\d*)"
        delivery_discount_pattern = r"diskon ongkir (\d+[.,]?\d*rb|\d+[.,]?\d*)"

        dim_promotion_base = promo_df.select(
            col("Judul Promo").alias("name"),
            col("Detail Promo").alias("description")
        ).distinct() 

        dim_promotion_base = dim_promotion_base.withColumn(
            "discount_percentage",
            regexp_extract(col("name"), discount_pattern, 1).cast("float") / 100
        ).withColumn(
            "max_discount",
            regexp_replace(
                regexp_replace(lower(regexp_extract(col("name"), max_discount_pattern, 1)), "rb", "000"),
                "\\.", ""
            ).cast("float")
        ).withColumn(
            "min_purchase",
            regexp_replace(
                regexp_replace(lower(regexp_extract(col("description"), min_purchase_pattern, 1)), "rb", "000"),
                "\\.", ""
            ).cast("float")
        ).withColumn(
            "additional_discount",
            regexp_replace(
                regexp_replace(lower(regexp_extract(col("description"), additional_discount_pattern, 1)), "rb", "000"),
                "\\.", ""
            ).cast("float")
        ).withColumn(
            "delivery_discount",
            regexp_replace(
                regexp_replace(lower(regexp_extract(col("description"), delivery_discount_pattern, 1)), "rb", "000"),
                "\\.", ""
            ).cast("float")
        )

        numeric_cols = ["discount_percentage", "max_discount", "min_purchase", "additional_discount", "delivery_discount"]
        dim_promotion_base = dim_promotion_base.na.fill(0.0, subset=numeric_cols)

        # print(f"DEBUG: _create_dim_promotion - dim_promotion_base count: {dim_promotion_base.count()}")

        max_id_existing = self._get_max_id_from_bigquery("dim_promotion", "id_promo")
        
        window_spec = Window.orderBy("name", "description", "discount_percentage", "max_discount", "min_purchase", "additional_discount", "delivery_discount")
        dim_promotion_final = dim_promotion_base.withColumn("row_num", dense_rank().over(window_spec).cast(LongType()))
        dim_promotion_final = dim_promotion_final.withColumn("id_promo", col("row_num") + lit(max_id_existing)) \
                                     .drop("row_num")

        dim_promotion_final = dim_promotion_final.withColumn("status", lit("aktif").cast(StringType()))
        dim_promotion_final = dim_promotion_final.withColumn(
            "active_until_date",
            date_add(lit(self.run_date).cast(DateType()), 1)
        )
        # print(f"DEBUG: dim_promotion final count: {dim_promotion_final.count()}")
        return dim_promotion_final.select(
            "id_promo", "name", "description", "discount_percentage", 
            "max_discount", "min_purchase", "additional_discount", "delivery_discount",
            "status", "active_until_date"
        )
    

    def _create_dim_date(self) -> DataFrame:
        """
        Creates the 'Dim_Date' dimensional table.

        This method extracts unique dates from the 'ulasan_df' dataset's
        "Tanggal Beli" column, converts them to DateType, and then enriches
        them with various date attributes like day, day name, month, month name,
        quarter, year, and a boolean indicating if it's a weekend.
        It generates unique surrogate keys ('id_time') by incorporating the
        maximum existing ID from BigQuery.

        Returns:
            DataFrame: A Spark DataFrame representing the 'Dim_Date' table
                       with columns: 'id_time', 'date', 'day', 'day_name',
                       'month', 'month_name', 'quarter', 'year', 'is_weekend'.
        """
        print("Create Dim_Time...")
        ulasan_df = self.datasets['ulasan_df']
        # print(f"DEBUG: _create_dim_date - ulasan_df count: {ulasan_df.count()}")
        
        date_df = ulasan_df.withColumn(
            "date_sql",
            to_date(col("Tanggal Beli"), "d MMMM yyyy").cast(DateType()) 
        ).select("date_sql").distinct()
        
        date_df = date_df.filter(col("date_sql").isNotNull()) # Filter out rows where date parsing failed
        
        # print(f"DEBUG: _create_dim_date - date_df count after parsing and distinct: {date_df.count()}")

        max_id_existing = self._get_max_id_from_bigquery("dim_date", "id_time")
        
        window_spec = Window.orderBy(col("date_sql"))
        dim_date = date_df.withColumn("row_num", dense_rank().over(window_spec).cast(LongType()))
        dim_date = dim_date.withColumn("id_time", col("row_num") + lit(max_id_existing)) \
            .withColumn("date", col("date_sql")) \
            .withColumn("day", dayofmonth(col("date_sql"))) \
            .withColumn("day_name", date_format(col("date_sql"), "EEEE")) \
            .withColumn("month", month(col("date_sql"))) \
            .withColumn("month_name", date_format(col("date_sql"), "MMMM")) \
            .withColumn("quarter", quarter(col("date_sql"))) \
            .withColumn("year", year(col("date_sql"))) \
            .withColumn("is_weekend", when(date_format(col("date_sql"), "E").isin(["Sat", "Sun"]), True).otherwise(False)) \
            .drop("row_num")
        # print(f"DEBUG: _create_dim_date - dim_date final count: {dim_date.count()}")
        return dim_date.select("id_time", "date", "day", "day_name", "month", "month_name", "quarter", "year", "is_weekend")


    def _create_fact_transaction(self, dim_restaurant: DataFrame, dim_menu: DataFrame, dim_date: DataFrame) -> DataFrame:
        """
        Creates the 'Fact_Transaction' fact table.

        This method processes the 'ulasan_df' and 'menu_df' datasets and joins them
        with the previously created dimensional tables (Dim_Restaurant, Dim_Menu, Dim_Date)
        to form the fact table. It extracts individual menu items and their prices,
        calculates transaction details like original price and rating, and assigns
        relevant foreign keys from the dimensional tables. It also generates
        unique surrogate keys ('TransactionID') by incorporating the maximum
        existing ID from BigQuery.

        Parameters:
            dim_restaurant (DataFrame): The 'Dim_Restaurant' DataFrame.
            dim_menu (DataFrame): The 'Dim_Menu' DataFrame.
            dim_date (DataFrame): The 'Dim_Date' DataFrame.

        Returns:
            DataFrame: A Spark DataFrame representing the 'Fact_Transaction' table
                       with columns: 'TransactionID', 'id_time', 'id_restaurant',
                       'id_menu', 'id_promo', 'quantity', 'original_price',
                       'discount_amount', 'price_after_promo', 'rating'.
        """
        print("Create Fact_Transaction...")
        ulasan_df = self.datasets['ulasan_df']
        menu_df = self.datasets['menu_df']
        
        # print(f"DEBUG: _create_fact_transaction - ulasan_df count: {ulasan_df.count()}")
        # print(f"DEBUG: _create_fact_transaction - menu_df count: {menu_df.count()}")
        # print(f"DEBUG: _create_fact_transaction - dim_restaurant count (input): {dim_restaurant.count()}")
        # print(f"DEBUG: _create_fact_transaction - dim_menu count (input): {dim_menu.count()}")
        # print(f"DEBUG: _create_fact_transaction - dim_date count (input): {dim_date.count()}")

        transaksi_base = ulasan_df.withColumn("menu_name", explode(split(col("Produk yang Dibeli"), " - "))) \
            .withColumn("menu_name_clean", trim(lower(col("menu_name")))) \
            .filter(col("menu_name_clean").isNotNull() & (col("menu_name_clean") != "")) \
            .select("Nama Restoran", "Tanggal Beli", "Rating", "menu_name_clean")

        # print(f"DEBUG: _create_fact_transaction - transaksi_base count: {transaksi_base.count()}")

        dim_menu_join = dim_menu.withColumn("name_lower", lower(col("name"))).select("id_menu", "name_lower")
        menu_price_df_join = menu_df.select(
            col("Nama Restoran").alias("resto_name"),
            lower(col("Nama Menu")).alias("menu_name_price"),
            (col("Harga").cast(FloatType()) * 1000.0).alias("OriginalPrice")
        ).distinct()
        
        # print(f"DEBUG: _create_fact_transaction - dim_menu_join count: {dim_menu_join.count()}")
        # print(f"DEBUG: _create_fact_transaction - menu_price_df_join count: {menu_price_df_join.count()}")

        transaksi_base_with_date = transaksi_base.withColumn(
            "Tanggal Beli_date", to_date(col("Tanggal Beli"), "d MMMM yyyy") # Format dan locale baru
        ).filter(col("Tanggal Beli_date").isNotNull())

        # print(f"DEBUG: _create_fact_transaction - transaksi_base_with_date count after date parsing: {transaksi_base_with_date.count()}")

        transaksi_enriched = transaksi_base_with_date \
            .join(dim_restaurant, transaksi_base_with_date["Nama Restoran"] == dim_restaurant["name"], "inner")
        # print(f"DEBUG: _create_fact_transaction - count after dim_restaurant join: {transaksi_enriched.count()}")

        transaksi_enriched = transaksi_enriched.join(dim_menu_join, transaksi_base_with_date["menu_name_clean"] == dim_menu_join["name_lower"], "inner")
        # print(f"DEBUG: _create_fact_transaction - count after dim_menu join: {transaksi_enriched.count()}")

        transaksi_enriched = transaksi_enriched.join(dim_date, transaksi_base_with_date["Tanggal Beli_date"] == dim_date["date"], "inner")
        # print(f"DEBUG: _create_fact_transaction - count after dim_date join: {transaksi_enriched.count()}")

        transaksi_enriched = transaksi_enriched.join(
                menu_price_df_join,
                (transaksi_base_with_date["Nama Restoran"] == menu_price_df_join["resto_name"]) &
                (transaksi_base_with_date["menu_name_clean"] == menu_price_df_join["menu_name_price"]),
                "left"
            )
        # print(f"DEBUG: _create_fact_transaction - count after menu_price_df_join: {transaksi_enriched.count()}")

        max_id_existing = self._get_max_id_from_bigquery("fact_transaction", "TransactionID")

        window_spec_fact = Window.orderBy(col("Nama Restoran"), col("Tanggal Beli_date"), col("menu_name_clean"))
        fact_transaction = transaksi_enriched.withColumn("row_num", dense_rank().over(window_spec_fact).cast(LongType()))
        
        fact_transaction = fact_transaction.select(
            (col("row_num") + lit(max_id_existing)).alias("TransactionID"),
            col("id_time"),
            col("id_restaurant"),
            col("id_menu"),
            lit(-1).alias("id_promo"),
            lit(1).cast(IntegerType()).alias("quantity"),
            (regexp_replace(col("OriginalPrice"), "[^0-9]", "").cast(FloatType()) / 100).alias("original_price"),
            lit(0.0).cast(FloatType()).alias("discount_amount"),
            (regexp_replace(col("OriginalPrice"), "[^0-9]", "").cast(FloatType()) / 100).alias("price_after_promo"),
            col("Rating").alias("rating").cast(FloatType())
        ).na.fill({"original_price": 0, "rating": 0.0}).filter(col("original_price") > 0)
        
        # print(f"DEBUG: _create_fact_transaction - fact_transaction final count: {fact_transaction.count()}")
        return fact_transaction


    def _save_tables(self, tables: dict) -> None:
        """
        Saves the transformed Spark DataFrames to Google Cloud Storage (GCS)
        in Parquet format.

        Each DataFrame in the provided dictionary is saved to its corresponding
        path within the `self.gcs_output_path` directory, overwriting any
        existing files at that location.

        Parameters:
            tables (dict): A dictionary where keys are table names (e.g., "dim_restaurant")
                           and values are the Spark DataFrames to be saved.
        """
        print(f"\nSaving tables to GCS Silver Layer: {self.gcs_output_path}")
        for name, df in tables.items():
            output_path = f"{self.gcs_output_path}/{name}"
            df.write.mode("overwrite").parquet(output_path)
            print(f"✅ The table ‘{name}’ has been successfully saved in {output_path}")


    def run(self) -> None:
        """
        Executes the entire ETL pipeline.

        This method orchestrates the creation of all dimensional tables
        (Dim_Restaurant, Dim_Menu, Dim_Promotion, Dim_Date) and then
        the fact table (Fact_Transaction). Finally, it saves all the
        generated tables to GCS.
        """
        dim_restaurant = self._create_dim_restaurant()
        dim_menu = self._create_dim_menu()
        dim_promotion = self._create_dim_promotion()
        dim_date = self._create_dim_date()
        
        fact_transaction = self._create_fact_transaction(dim_restaurant, dim_menu, dim_date)

        final_tables = {
            "dim_restaurant": dim_restaurant,
            "dim_menu": dim_menu,
            "dim_promotion": dim_promotion,
            "dim_date": dim_date,
            "fact_transaction": fact_transaction
        }
        
        self._save_tables(final_tables)