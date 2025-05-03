import csv
import urllib.request

def extract_csv(url, filename):
   # Download data dari URL
   urllib.request.urlretrieve(url, filename)

   # Membaca data CSV
   with open(filename, "r") as f:
       reader = csv.DictReader(f)
       data   = [row for row in reader]
   return data

# Panggil fungsi
url            = "https://raw.githubusercontent.com/codeforamerica/ohana-api/master/data/sample-csv/addresses.csv"
filename       = "addresses.csv"
data_airtravel = extract_csv(url, filename)

print(data_airtravel)