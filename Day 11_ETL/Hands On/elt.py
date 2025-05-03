import bs4
import requests
import pandas as pd
import sqlalchemy as sa

def extract_news(url):
   # Mengambil konten halaman web
   response = requests.get(url)
   response.raise_for_status()  # Memeriksa jika permintaan berhasil

   # Menginisialisasi BeautifulSoup untuk parsing HTML
   soup = bs4.BeautifulSoup(response.content, "html.parser")

   # Mengambil semua teks dari tag <h2> yang biasanya berisi judul berita
   data = [h2.get_text() for h2 in soup.find_all("h2")]

   # Membuat DataFrame dari data yang diekstraksi
   df = pd.DataFrame(data, columns=["title"])

   return df

def load_sqlite(df, table_name, engine):
   # Load DataFrame ke tabel SQLite, menggantikan tabel jika sudah ada
   with engine.begin() as conn:
       df.to_sql(table_name, conn, index=False, if_exists="replace")



def transform_uppercase(raw_table_name, table_name, engine):
   # Membuat tabel baru dengan judul berita dalam huruf besar
   with engine.begin() as conn:
       conn.execute(sa.text(f"DROP TABLE IF EXISTS {table_name}"))
       conn.execute(sa.text(f"""
           CREATE TABLE {table_name} AS
           SELECT UPPER(title) as title
           FROM {raw_table_name}
       """))

# Konfigurasi
url            = "https://www.bbc.com/news"
table_name     = "elt_news"
table_name_raw = "elt_news_raw"
engine         = sa.create_engine(f"sqlite:///dibimbing.sqlite")

# Extract data berita
df_news = extract_news(url)
print("Extract berhasil")

# Load data berita ke SQLite
load_sqlite(df_news, table_name_raw, engine)
print("Load berhasil")

# Transformasi data berita menjadi huruf besar dan simpan ke tabel baru
transform_uppercase(table_name_raw, table_name, engine)
print("Transform berhasil")


