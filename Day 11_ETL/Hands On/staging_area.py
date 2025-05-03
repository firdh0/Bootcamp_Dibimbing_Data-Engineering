import argparse
import pandas as pd
import sqlalchemy as sa

pd.options.display.max_columns = None

def get_last_id(table_name, engine):
   with engine.begin() as conn:
       df = pd.read_sql(sa.text(f"SELECT MAX(id) FROM {table_name}"), conn)
       return df.iloc[0, 0]

def extract(filepath, staging_filepath, ingest_type, last_id):
   df = pd.read_json(filepath)

   if ingest_type == "incremental" and last_id is not None:
       df = df[df.id > last_id]

   # Menyimpan DataFrame ke file staging
   df.to_parquet(staging_filepath, index=False)

def load(staging_filepath, table_name, ingest_type, engine):
   # Membaca data staging
   df = pd.read_parquet(staging_filepath)

   if ingest_type == "incremental" and last_id is not None:
       if_exists = "append"
   else:
       if_exists = "replace"

   with engine.begin() as conn:
       df.to_sql(table_name, conn, index=False, if_exists=if_exists)


# Konfigurasi
parser = argparse.ArgumentParser()
parser.add_argument("--filepath")
parser.add_argument("--table")
parser.add_argument("--ingest_type", choices=['full', 'incremental'])
args = parser.parse_args()

engine = sa.create_engine("sqlite:///dibimbing.sqlite")

# Mendapatkan ID terakhir jika ingest_type incremental
if args.ingest_type == "incremental":
   last_id = get_last_id(args.table, engine)
   print(f"last id: {last_id}")
else:
   last_id = None

# generate staging filepath
staging_filepath = f"staging_{args.table}_{args.ingest_type}_{last_id}.parquet"

# Extract data dari file JSON
extract(args.filepath, staging_filepath, args.ingest_type, last_id)
print("Extract berhasil")

# Load data ke SQLite
load(staging_filepath, args.table, args.ingest_type, engine)
print("Load berhasil")

# contoh:
# python staging_area.py --filepath posts.json --table posts --ingest_type full
# python staging_area.py --filepath posts.json --table posts --ingest_type incremental



