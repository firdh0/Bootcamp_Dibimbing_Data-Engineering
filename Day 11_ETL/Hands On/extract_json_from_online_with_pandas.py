import pandas as pd

pd.options.display.max_columns = None

def extract_json_from_online_with_pandas(url): # Sama seperti IO tanpa perlu download csv untuk baca filenya
   # Mengambil data dari URL
   df = pd.read_json(url)
   return df

# Panggil fungsi
url      = "https://jsonplaceholder.typicode.com/posts"
df_posts = extract_json_from_online_with_pandas(url)

print(df_posts)


