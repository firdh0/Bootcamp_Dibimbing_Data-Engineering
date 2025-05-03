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

def transform_uppercase(df):
   # Membuat judul berita menjadi huruf besar
   df.title = df.title.str.upper()

   return df

# Konfigurasi
url        = "https://www.bbc.com/news"
table_name = "etl_news"
engine     = sa.create_engine(f"sqlite:///dibimbing.sqlite")

# Extract data berita
df_news = extract_news(url)
print("Extract berhasil")

# Transformasi data berita menjadi huruf besar
df_news_transformed = transform_uppercase(df_news)
print("Transform berhasil")

# Load data berita ke SQLite
load_sqlite(df_news_transformed, table_name, engine)
print("Load berhasil")








