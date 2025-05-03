import io
import csv
import requests

def extract_csv_from_memory(url):
   # Mengambil data dari URL
   response = requests.get(url)
   content  = response.content.decode("utf-8")

   # Membaca data CSV dari memory
   with io.StringIO(content) as f: # menyimpan data di CPU kemudian diproses via RAM
       reader = csv.reader(f)
       data   = [row for row in reader]
   return data

# Panggil fungsi
url            = "https://raw.githubusercontent.com/codeforamerica/ohana-api/master/data/sample-csv/addresses.csv"
data_airtravel = extract_csv_from_memory(url)

print(data_airtravel)


