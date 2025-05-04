import os
from flask import Flask

app = Flask(__name__)

# Mendapatkan nilai argumen CUSTOM_NAME dari environment variable
custom_name = os.getenv('CUSTOM_NAME', 'my-python-app')

@app.route('/')
def hello_world():
    return f'Hello, {custom_name}!'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
