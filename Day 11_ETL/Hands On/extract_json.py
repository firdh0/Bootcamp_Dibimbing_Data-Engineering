import json
import urllib.request

def extract_json(url, filename):
   # Download data dari URL
   urllib.request.urlretrieve(url, filename)

   # Membaca data json
   with open(filename, "r") as f:
       reader = json.load(f)
       data   = [row for row in reader]
   return data

# Panggil fungsi
url        = "https://jsonplaceholder.typicode.com/posts"
filename   = "posts.json"
data_posts = extract_json(url, filename)

print(data_posts)


