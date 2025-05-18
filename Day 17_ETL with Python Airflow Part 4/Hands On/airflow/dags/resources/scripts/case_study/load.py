def load_table(table, **kwargs):
   import pandas as pd
   from airflow.providers.postgres.hooks.postgres import PostgresHook

   data_interval_start = kwargs['data_interval_start']
   data_interval_end   = kwargs['data_interval_end']
   ingestion_mode      = kwargs["params"][table]

   df = pd.read_parquet(f"data/case_study/{table}/{data_interval_start}_{data_interval_end}_{ingestion_mode}.parquet")

   engine = PostgresHook("postgres_dibimbing").get_sqlalchemy_engine()
   with engine.connect() as conn:
       # Menentukan mode penulisan ke tabel berdasarkan tipe sinkronisasi (incremental atau replace)
       if ingestion_mode == "incremental":
           if_exists = "append"
       else:
           if_exists = "replace"

       # Menulis DataFrame ke tabel SQL di schema "bronze" dengan mode yang ditentukan
       df.to_sql(table, conn, schema="bronze", index=False, if_exists=if_exists)


