def extract_table(table, **kwargs):
   import os
   import pandas as pd
   from airflow.providers.mysql.hooks.mysql import MySqlHook

   data_interval_start = kwargs['data_interval_start']
   data_interval_end   = kwargs['data_interval_end']
   ingestion_mode      = kwargs["params"][table]

   engine = MySqlHook("mysql_dibimbing").get_sqlalchemy_engine()
   with engine.connect() as conn:

       # Mengambil kolom dengan tipe data terkait timestamp dari tabel yang ditentukan
       timestamp_cols = pd.read_sql(f"""
           SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA = 'case_study'
               AND DATA_TYPE IN ('date', 'datetime', 'timestamp')
               AND TABLE_NAME = '{table}'
       """, conn).COLUMN_NAME.tolist()

       print("Kolom timestamp:", timestamp_cols)

       # Membuat query SQL untuk mengekstrak data dari tabel dengan menambahkan kondisi tambahan
       query = f"SELECT *, CURRENT_TIMESTAMP AS md_extracted_at FROM case_study.{table}"
       if ingestion_mode == "incremental" and timestamp_cols:
           query += " WHERE "
           query += " OR ".join(f"{col} BETWEEN '{data_interval_start}' AND '{data_interval_end}'" for col in timestamp_cols)

       print("Query:", query)

       # Menjalankan query SQL dan memuat hasilnya ke dalam DataFrame pandas
       df = pd.read_sql(query, conn)
       print("DataFrame:", df)

   os.makedirs(f"data/case_study/{table}", exist_ok=True)
   df.to_parquet(f"data/case_study/{table}/{data_interval_start}_{data_interval_end}_{ingestion_mode}.parquet", index=False)


