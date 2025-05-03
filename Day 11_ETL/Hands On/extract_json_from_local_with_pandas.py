import pandas as pd
import urllib.request

pd.options.display.max_columns = None

def extract_json_from_local_with_pandas(url, filename):
   # Download data dari URL
   urllib.request.urlretrieve(url, filename)

   # Membaca data json
   df = pd.read_json(filename)
   return df

# Panggil fungsi
url      = "https://jsonplaceholder.typicode.com/posts"
filename = "posts.json"
df_posts = extract_json_from_local_with_pandas(url, filename)

print(df_posts)


