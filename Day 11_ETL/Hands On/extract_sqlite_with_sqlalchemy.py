import pandas as pd
import urllib.request
import sqlalchemy as sa

pd.options.display.max_columns = None

def extract_sqlite_with_sqlalchemy(url, filename, query):
   # Download database dari URL
   urllib.request.urlretrieve(url, filename)

   # Membuat koneksi ke database SQLite
   engine = sa.create_engine(f"sqlite:///{filename}")

   # Mengambil data dengan query SQL
   with engine.connect().execution_options(stream_results=True) as conn:
       df = pd.read_sql(sa.text(query), conn)

   return df

# Panggil fungsi
url       = "https://github.com/djv007/Project-IMDB-database/raw/master/IMDB.sqlite"
filename  = "imdb.sqlite"
query     = "SELECT * FROM IMDB LIMIT 10"
df_sqlite = extract_sqlite_with_sqlalchemy(url, filename, query)

print(df_sqlite)


