import requests

def extract_api(url):
   # Mengambil data dari API
   response = requests.get(url)
   response.raise_for_status()
 
   # Convert data ke json
   data = response.json()
   return data

# Panggil fungsi
url         = "https://api.spacexdata.com/v4/launches/latest"
data_spacex = extract_api(url)

print(data_spacex)


