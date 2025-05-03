import argparse
import pandas as pd
import sqlalchemy as sa

pd.options.display.max_columns = None

# Function untuk mendapatkan ID terakhir dari tabel SQLite.
def get_last_id(table_name, engine):
   with engine.begin() as conn:
       # Mendapatkan ID maksimum dari tabel
       df = pd.read_sql(sa.text(f"SELECT MAX(id) FROM {table_name}"), conn)
       return df.iloc[0, 0]

# Function untuk mengekstrak data dari file JSON.
def extract(filepath, ingest_type, last_id):
   df = pd.read_json(filepath)

   if ingest_type == "incremental" and last_id is not None:
       # Memfilter data untuk hanya mengambil data yang ID-nya lebih besar dari last_id
       df = df[df.id > last_id]

   return df

# Function untuk me-load DataFrame ke dalam tabel SQLite.
def load(df, table_name, ingest_type, engine):
   # Mengganti tabel jika ingest_type full atau menambahkan data ke tabel jika ingest_type incremental
   if ingest_type == "incremental" and last_id is not None:
       if_exists = "append"
   else:
       if_exists = "replace"

   with engine.begin() as conn:
       # Load DataFrame ke dalam tabel SQLite
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

# Extract data dari file JSON
df = extract(args.filepath, args.ingest_type, last_id)
print(df)
print("Extract berhasil")

# Load data ke SQLite
load(df, args.table, args.ingest_type, engine)
print("Load berhasil")

# contoh:
# python ingestion_mode.py --filepath posts.json --table posts --ingest_type full
# python ingestion_mode.py --filepath posts.json --table posts --ingest_type incremental



